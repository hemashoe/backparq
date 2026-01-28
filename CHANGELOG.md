# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-01-28

### Added

- Rich console output with progress bars (replaced tqdm with rich)
- `verify` command to check archive integrity with optional `--repair`
- `init` command for interactive configuration generation
- `--stats` flag for archive command to display statistics
- `--output json` flag for machine-readable output
- Progress tracking file (`progress.json`) for resumability
- `ArchiveResult` and `VerifyResult` dataclasses
- Custom exception hierarchy (`BackparqError`, `ArchiveError`, etc.)

### Changed

- Replaced emojis with text labels (OK, WARN, ERROR, INFO)
- Improved README with comprehensive documentation
- Reduced verbose logging and comments
- Better exception chaining with `from e`

### Fixed

- B904: Added proper exception chaining in cli.py and config.py
- C414: Removed unnecessary list() in sorted() call
- Fixed all ruff import sorting and unused import warnings

## [0.1.0] - 2026-01-28

### Added

- Initial release
- Archive PostgreSQL tables to Parquet files on S3
- Two modes: `offload` (move old data) and `backup` (full snapshots)
- Per-table primary key configuration for restore upserts
- Optional Parquet encryption with column-level keys
- SHA256 checksum verification before data deletion
- Graceful shutdown on SIGINT/SIGTERM signals
- Configurable logging verbosity (`-v`, `-vv`)
- Retention-based pruning of old backups
- Parallel processing at table and chunk level
- Server-side cursor streaming for constant memory usage

### Security

- SQL injection prevention using parameterized identifiers
- Support for S3 server-side encryption (SSE-S3, SSE-KMS)
- Optional client-side Parquet encryption

[0.2.0]: https://github.com/hemashoe/backparq/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/hemashoe/backparq/releases/tag/v0.1.0
