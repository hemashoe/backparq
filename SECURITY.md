# Security Policy

## Supported Versions

| Version | Supported |
| ------- | --------- |
| 0.4.x   | Yes       |
| 0.3.x   | Yes       |
| < 0.3   | No        |

## Reporting Vulnerabilities

1. Do not open a public GitHub issue.
2. Email the maintainers (see pyproject.toml for contact).
3. Include description, steps to reproduce, and impact.

We will acknowledge within 48 hours and assess within 7 days.

## Data Handling

- Backparq runs in your infrastructure. No data is sent externally.
- Credentials are read from config and environment; never logged or transmitted.
- Use IAM roles and environment variables instead of storing credentials in config files.
