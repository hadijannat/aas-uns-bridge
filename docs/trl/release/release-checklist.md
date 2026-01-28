# Release Validation Checklist

**Project:** aas-uns-bridge
**Version:** [X.Y.Z]
**Release Date:** YYYY-MM-DD
**Release Manager:** [Name]

## Pre-Release Checklist

### Code Quality

- [ ] All CI checks pass on main branch
- [ ] Code coverage ≥ 80%
- [ ] No critical or high security vulnerabilities (dependency scan)
- [ ] Ruff linting passes with no errors
- [ ] Mypy type checking passes

### Testing

- [ ] Unit tests pass (291+ tests)
- [ ] Integration tests pass
- [ ] E2E tests pass
- [ ] Sparkplug compliance tests pass
- [ ] QoS enforcement tests pass (`test_sparkplug_qos_compliance.py`)

### Documentation

- [ ] README.md updated with new features
- [ ] CHANGELOG.md updated
- [ ] API documentation current
- [ ] Configuration examples updated
- [ ] TRL evidence pack current

### TRL Evidence

- [ ] Requirements specification reviewed
- [ ] Verification matrix updated with any new tests
- [ ] Sparkplug compliance checklist verified
- [ ] Interoperability matrix updated (if new brokers tested)

## Release Steps

### 1. Version Bump

- [ ] Update version in `pyproject.toml`
- [ ] Update version in `__init__.py`
- [ ] Create release tag: `git tag -a vX.Y.Z -m "Release X.Y.Z"`

### 2. Changelog

- [ ] Generate changelog from git commits
- [ ] Categorize changes (Features, Fixes, Breaking Changes)
- [ ] Add migration notes if needed

### 3. Build

- [ ] Build wheel: `python -m build`
- [ ] Verify wheel contents
- [ ] Test wheel installation in clean environment

### 4. Test Release (Test PyPI)

- [ ] Upload to Test PyPI
- [ ] Install from Test PyPI and verify
- [ ] Run smoke tests against Test PyPI package

### 5. Production Release

- [ ] Upload to PyPI: `twine upload dist/*`
- [ ] Verify PyPI listing
- [ ] Update documentation site (if applicable)
- [ ] Create GitHub release with changelog

### 6. Post-Release

- [ ] Announce release (if applicable)
- [ ] Update any dependent projects
- [ ] Monitor for immediate issues

## Release Validation Tests

### Smoke Tests

```bash
# Install from PyPI
pip install aas-uns-bridge==X.Y.Z

# Verify version
python -c "import aas_uns_bridge; print(aas_uns_bridge.__version__)"

# Validate configuration
aas-uns-bridge validate --config config/config.example.yaml

# Quick connectivity test (requires broker)
# aas-uns-bridge run --config config/config.example.yaml --once
```

### Functional Verification

- [ ] AAS file ingestion works
- [ ] UNS messages published with retain=true
- [ ] Sparkplug births published correctly
- [ ] Reconnection after broker restart
- [ ] Graceful shutdown

## Rollback Plan

If critical issues discovered post-release:

1. Yank the PyPI release (if necessary)
2. Notify users via GitHub issue/announcement
3. Create hotfix branch from previous release
4. Apply fix and release patch version

## Approvals

| Role | Name | Date | Signature |
|------|------|------|-----------|
| Release Manager | | | |
| Technical Lead | | | |
| QA | | | |

## Release Notes Template

```markdown
## [X.Y.Z] - YYYY-MM-DD

### Added
- Feature 1
- Feature 2

### Changed
- Change 1
- Change 2

### Fixed
- Fix 1
- Fix 2

### Security
- Security fix 1

### Breaking Changes
- Breaking change 1

### Migration Guide
[Instructions for upgrading from previous version]
```

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-01-28 | TRL Evidence Pack | Initial checklist |
