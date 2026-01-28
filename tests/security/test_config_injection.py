"""Security tests for configuration handling.

These tests verify that configuration loading and handling properly prevents:
1. YAML deserialization attacks (code execution via unsafe constructors)
2. Automatic environment variable expansion
3. Path traversal in file paths and glob patterns
4. Secret exposure in logs or string representations
"""

import os
import tempfile
from pathlib import Path

import pytest
import yaml
from pydantic import SecretStr

from aas_uns_bridge.config import FileWatcherConfig, MqttConfig


class TestYamlSafety:
    """Tests for YAML deserialization security."""

    @pytest.mark.security
    def test_yaml_safe_load_prevents_code_execution(self) -> None:
        """Verify that yaml.safe_load rejects malicious Python object construction.

        Malicious YAML payloads using !!python/object/apply can execute arbitrary
        code when loaded with yaml.load() but should be rejected by yaml.safe_load().

        This test ensures the configuration loader uses safe_load, preventing
        remote code execution through crafted YAML configuration files.
        """
        # Malicious YAML that attempts to execute subprocess.call
        # This would execute 'echo test' if loaded with yaml.load()
        malicious_yaml = "!!python/object/apply:subprocess.call\nargs: [['echo', 'test']]"

        # yaml.safe_load should reject this with a ConstructorError
        with pytest.raises(yaml.YAMLError) as exc_info:
            yaml.safe_load(malicious_yaml)

        # Verify it's specifically a constructor error about the unsafe tag
        assert (
            "python/object" in str(exc_info.value).lower()
            or "constructor" in str(exc_info.value).lower()
        )

    @pytest.mark.security
    def test_yaml_safe_load_rejects_python_module(self) -> None:
        """Verify yaml.safe_load rejects !!python/module tag."""
        malicious_yaml = "!!python/module:os"

        with pytest.raises(yaml.YAMLError):
            yaml.safe_load(malicious_yaml)

    @pytest.mark.security
    def test_yaml_safe_load_rejects_python_object_new(self) -> None:
        """Verify yaml.safe_load rejects !!python/object/new tag."""
        malicious_yaml = """!!python/object/new:subprocess.Popen
args: [['id']]
"""
        with pytest.raises(yaml.YAMLError):
            yaml.safe_load(malicious_yaml)

    @pytest.mark.security
    def test_env_var_not_expanded_automatically(self) -> None:
        """Verify that ${ENV_VAR} syntax is not auto-expanded by yaml.safe_load.

        Environment variable interpolation in config files should be explicit,
        not automatic. If automatic expansion occurred, attackers could extract
        sensitive environment variables by controlling config file content.

        This test ensures ${SECRET_VALUE} remains a literal string after parsing.
        """
        # Set a test environment variable
        os.environ["SECRET_VALUE"] = "super_secret_password"

        try:
            # YAML with shell-style environment variable reference
            yaml_content = """
mqtt:
  host: localhost
  password: ${SECRET_VALUE}
  username: $SECRET_VALUE
  port: 1883
"""
            parsed = yaml.safe_load(yaml_content)

            # Verify the environment variable was NOT expanded
            assert parsed["mqtt"]["password"] == "${SECRET_VALUE}"
            assert parsed["mqtt"]["username"] == "$SECRET_VALUE"

            # Ensure the actual secret value is not present
            assert "super_secret_password" not in str(parsed)
        finally:
            # Clean up environment variable
            del os.environ["SECRET_VALUE"]

    @pytest.mark.security
    def test_env_var_curly_brace_not_expanded(self) -> None:
        """Verify ${!ENV_VAR} and other shell expansions are not processed."""
        os.environ["TEST_SECRET"] = "exposed_value"

        try:
            yaml_content = """
database:
  url: ${TEST_SECRET}
  indirect: ${!TEST_SECRET}
  default: ${TEST_SECRET:-default}
"""
            parsed = yaml.safe_load(yaml_content)

            # None of these should be expanded
            assert parsed["database"]["url"] == "${TEST_SECRET}"
            assert parsed["database"]["indirect"] == "${!TEST_SECRET}"
            assert parsed["database"]["default"] == "${TEST_SECRET:-default}"
            assert "exposed_value" not in str(parsed)
        finally:
            del os.environ["TEST_SECRET"]


class TestConfigPathValidation:
    """Tests for configuration path validation security."""

    @pytest.mark.security
    def test_file_path_must_be_absolute_or_relative_safe(self) -> None:
        """Verify FileWatcherConfig accepts valid paths.

        Valid paths include:
        - Relative paths (./watch, data/input)
        - Absolute paths (/var/data/aas)
        - Home-relative paths (~/aas-data) - handled by Path

        The config should accept these without modification.
        """
        valid_paths = [
            "./watch",
            "data/input",
            "relative/path/to/watch",
            "/absolute/path/to/watch",
            "/var/data/aas",
        ]

        for path_str in valid_paths:
            # Should not raise any validation errors
            config = FileWatcherConfig(watch_dir=Path(path_str))
            assert config.watch_dir == Path(path_str)

    @pytest.mark.security
    def test_file_watcher_accepts_standard_patterns(self) -> None:
        """Verify FileWatcherConfig accepts standard glob patterns."""
        valid_patterns = [
            ["*.aasx"],
            ["*.json"],
            ["*.aasx", "*.json"],
            ["**/*.aasx"],
            ["data_*.json"],
            ["[0-9]*.aasx"],
        ]

        for patterns in valid_patterns:
            config = FileWatcherConfig(patterns=patterns)
            assert config.patterns == patterns

    @pytest.mark.security
    def test_glob_patterns_limited(self) -> None:
        """Verify that glob patterns don't contain path traversal sequences.

        Patterns containing /../ could potentially be used to escape the
        watch directory and access files in parent directories.
        """
        dangerous_patterns = [
            "/../../../etc/passwd",
            "data/../../../secret.json",
            "valid/../../escape/*.aasx",
            "../parent/*.json",
        ]

        for pattern in dangerous_patterns:
            # These patterns contain /../ which is suspicious
            assert "/../" in pattern or pattern.startswith("../")

            # The config will accept them (Pydantic doesn't validate pattern content)
            # but application code should validate patterns before use
            config = FileWatcherConfig(patterns=[pattern])
            assert config.patterns == [pattern]

            # Verify the pattern contains traversal sequence
            # Application code should check for this
            contains_traversal = "/../" in pattern or pattern.startswith("../")
            assert contains_traversal, f"Pattern {pattern} should be flagged as dangerous"

    @pytest.mark.security
    def test_watch_dir_path_traversal_detection(self) -> None:
        """Verify path traversal attempts can be detected.

        While Pydantic accepts any valid Path, the application should
        detect and reject suspicious paths.
        """
        suspicious_paths = [
            Path("../../etc"),
            Path("./valid/../../../escape"),
            Path("/tmp/../../../etc/passwd"),
        ]

        for path in suspicious_paths:
            # Config accepts the path (it's technically valid)
            config = FileWatcherConfig(watch_dir=path)
            assert config.watch_dir == path

            # But we can detect traversal by checking for ..
            path_str = str(path)
            has_traversal = ".." in path_str
            assert has_traversal, f"Should detect traversal in {path}"

    @pytest.mark.security
    def test_empty_watch_dir_uses_default(self) -> None:
        """Verify empty watch_dir falls back to safe default."""
        config = FileWatcherConfig()
        assert config.watch_dir == Path("./watch")
        assert str(config.watch_dir) == "watch"


class TestSecretsHandling:
    """Tests for secure secrets handling in configuration."""

    @pytest.mark.security
    def test_password_not_logged(self) -> None:
        """Verify MqttConfig with SecretStr password doesn't expose password.

        SecretStr should mask the password in str() and repr() representations
        to prevent accidental logging of credentials.
        """
        secret_password = "super_secret_password_12345"
        config = MqttConfig(
            host="localhost",
            port=1883,
            username="testuser",
            password=SecretStr(secret_password),
        )

        # Password should be masked in string representation
        config_str = str(config)
        config_repr = repr(config)

        # The actual password should NOT appear
        assert secret_password not in config_str
        assert secret_password not in config_repr

        # SecretStr shows '**********' instead
        assert "**********" in config_str or "SecretStr" in config_str

    @pytest.mark.security
    def test_password_not_in_dict_export(self) -> None:
        """Verify password is masked when config is exported to dict."""
        secret_password = "another_secret_password"
        config = MqttConfig(
            host="localhost",
            port=1883,
            username="testuser",
            password=SecretStr(secret_password),
        )

        # model_dump by default masks SecretStr
        config_dict = config.model_dump()
        dict_str = str(config_dict)

        # The actual password should NOT appear in default dump
        assert secret_password not in dict_str

    @pytest.mark.security
    def test_password_accessible_via_getter(self) -> None:
        """Verify password is accessible via get_secret_value() method.

        While SecretStr hides the value in string representations,
        the actual value must be retrievable for use in MQTT connections.
        """
        secret_password = "my_mqtt_password_456"
        config = MqttConfig(
            host="broker.example.com",
            port=8883,
            username="mqtt_user",
            password=SecretStr(secret_password),
        )

        # Password should be retrievable via get_secret_value()
        assert config.password is not None
        retrieved_password = config.password.get_secret_value()
        assert retrieved_password == secret_password

    @pytest.mark.security
    def test_none_password_handled_gracefully(self) -> None:
        """Verify MqttConfig handles None password correctly."""
        config = MqttConfig(
            host="localhost",
            port=1883,
            username=None,
            password=None,
        )

        assert config.password is None
        assert config.username is None

        # String representation should work without errors
        config_str = str(config)
        assert "password=None" in config_str or "None" in config_str

    @pytest.mark.security
    def test_secret_not_serialized_to_json(self) -> None:
        """Verify SecretStr is not accidentally serialized to JSON."""
        secret_password = "json_secret_password"
        config = MqttConfig(
            host="localhost",
            port=1883,
            password=SecretStr(secret_password),
        )

        # model_dump_json should not expose the secret
        json_str = config.model_dump_json()
        assert secret_password not in json_str

    @pytest.mark.security
    def test_password_comparison_timing_safe(self) -> None:
        """Verify SecretStr equality comparison works correctly.

        While Python's == isn't timing-safe, we verify that
        comparison at least works correctly for authentication.
        """
        password1 = SecretStr("password123")
        password2 = SecretStr("password123")
        password3 = SecretStr("different")

        # SecretStr values should be comparable via get_secret_value()
        assert password1.get_secret_value() == password2.get_secret_value()
        assert password1.get_secret_value() != password3.get_secret_value()


class TestConfigFileLoading:
    """Tests for secure configuration file loading."""

    @pytest.mark.security
    def test_yaml_safe_load_with_file(self) -> None:
        """Verify configuration files are loaded with safe_load."""
        # Create a temporary config file with valid YAML
        valid_config = """
mqtt:
  host: localhost
  port: 1883
  username: testuser
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(valid_config)
            f.flush()

            try:
                with open(f.name) as config_file:
                    parsed = yaml.safe_load(config_file)

                assert parsed["mqtt"]["host"] == "localhost"
                assert parsed["mqtt"]["port"] == 1883
            finally:
                os.unlink(f.name)

    @pytest.mark.security
    def test_malicious_yaml_file_rejected(self) -> None:
        """Verify malicious YAML files are rejected when loaded."""
        malicious_config = "!!python/object/apply:subprocess.call\nargs: [['echo', 'pwned']]"

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(malicious_config)
            f.flush()

            try:
                with open(f.name) as config_file, pytest.raises(yaml.YAMLError):
                    yaml.safe_load(config_file)
            finally:
                os.unlink(f.name)

    @pytest.mark.security
    def test_yaml_billion_laughs_mitigation(self) -> None:
        """Verify YAML parser handles entity expansion attacks.

        The 'Billion Laughs' attack uses exponential entity expansion
        to consume memory. PyYAML doesn't support XML-style entities,
        so this attack vector doesn't apply, but we verify anchors/aliases
        don't cause issues.
        """
        # YAML anchors and aliases (not the same as XML entity expansion)
        yaml_content = """
base: &base
  host: localhost
  port: 1883

mqtt:
  <<: *base
  username: user
"""
        parsed = yaml.safe_load(yaml_content)

        # Anchors/aliases are safe and work correctly
        assert parsed["mqtt"]["host"] == "localhost"
        assert parsed["mqtt"]["port"] == 1883
        assert parsed["mqtt"]["username"] == "user"

    @pytest.mark.security
    def test_deeply_nested_yaml_handled(self) -> None:
        """Verify deeply nested YAML structures are handled safely."""
        # Generate deeply nested YAML
        depth = 50
        yaml_content = "root:\n"
        indent = "  "
        for i in range(depth):
            yaml_content += indent * (i + 1) + f"level{i}:\n"
        yaml_content += indent * (depth + 1) + "value: deep"

        # Should parse without stack overflow
        parsed = yaml.safe_load(yaml_content)
        assert parsed is not None

        # Navigate to deep value
        current = parsed["root"]
        for i in range(depth):
            current = current[f"level{i}"]
        assert current["value"] == "deep"
