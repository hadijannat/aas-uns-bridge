# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |
| < 0.1   | :x:                |

## Reporting a Vulnerability

**Please do not open a public issue for security vulnerabilities.**

To report a security vulnerability, please email **security@example.com** with the following information:

1. **Description** - A clear description of the vulnerability
2. **Steps to Reproduce** - Detailed steps to reproduce the issue
3. **Impact Assessment** - Your assessment of the potential impact
4. **Suggested Fix** - If you have one, a suggested fix or mitigation

### What to Include

- Type of vulnerability (e.g., injection, authentication bypass, information disclosure)
- Full paths of affected source files
- Configuration required to reproduce
- Proof-of-concept or exploit code (if available)
- Impact of the vulnerability and potential attack scenarios

## Response Timeline

| Phase                    | Timeframe        |
| ------------------------ | ---------------- |
| Acknowledgment           | Within 48 hours  |
| Initial Assessment       | Within 1 week    |
| Coordinated Disclosure   | 90 days maximum  |

We will work with you to understand and validate the issue. Once validated, we will:

1. Develop a fix and test it thoroughly
2. Prepare a security advisory
3. Coordinate disclosure timing with you
4. Release the fix and publish the advisory

## Security Measures

The AAS-UNS Bridge implements several security measures:

### Input Validation

- All AAS content is validated against the BaSyx SDK schema
- Configuration files are validated at startup
- MQTT topic names are sanitized to prevent injection

### Authentication

- MQTT broker authentication via username/password
- Support for TLS client certificates (mTLS)
- Sparkplug B edge node authentication

### Network Security

- TLS encryption for all MQTT connections
- Support for private network deployment
- Configurable broker connection parameters

## Security Best Practices

For detailed security hardening guidance, see [docs/security-hardening.md](docs/security-hardening.md).
