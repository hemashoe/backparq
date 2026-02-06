# Changelog

All notable changes to this project will be documented in this file.

Format based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.0] - 2026-02-06

### Added

- `--version` (`-V`) flag to display package version
- `--quiet` (`-q`) flag to suppress progress output
- `read_parquet_batches()` for streaming reads of large files
- `get_parquet_schema()` to read schema without loading data
- Config file path logged at DEBUG level on startup

### Changed

- S3 client now has connection timeout (30s), read timeout (60s), and retry (3 attempts)
- `download_file()` retries on transient failures (5 attempts with exponential backoff)
- Moved F841 suppression to per-file ignore for `plan.py` only

### Fixed

- Race condition in lock acquisition using atomic file creation (`O_CREAT|O_EXCL`)
- `apply` command is now hidden from help output (deprecated)
- Removed unused variables in restore module

## [0.3.0] - 2026-02-02

### Added

- Structured logging with `log_with_data` for JSON output
- Connection pooling with configurable `max_pool_connections` (default: 50)
- S3 uploads include row counts in metadata
- Restore command logs row counts per chunk

### Changed

- Removed global signal handlers; pass `threading.Event` instead
- Cleaned up `archive.py` for better shutdown and resource management
- `max_pool_connections` auto-calculated from concurrency settings

### Fixed

- `archive` command was using `primary_key` instead of `order_by` column
- "Connection pool is full" warnings by properly sizing urllib3 pool
- Removed unused `S3Client` shim and legacy code

## [0.2.1] - 2026-01-28

### Fixed

- Lowered Python requirement to 3.8+ for older environments

## [0.2.0] - 2026-01-28

### Added

- Rich console output with progress bars
- `verify` command with optional `--repair`
- `init` command for interactive config generation
- `--stats` flag for archive statistics
- `--output json` for machine-readable output
- Progress tracking file (`progress.json`)
- `ArchiveResult` and `VerifyResult` dataclasses
- Custom exception hierarchy

### Changed

- Replaced emojis with text labels (OK, WARN, ERROR, INFO)
- Improved README documentation
- Reduced verbose logging

### Fixed

- Exception chaining in cli.py and config.py
- Removed unnecessary list() in sorted() call
- Import sorting and unused imports

## [0.1.0] - 2026-01-28

### Added

- Initial release
- Archive PostgreSQL tables to Parquet on S3
- Two modes: `offload` and `backup`
- Per-table primary key configuration
- Optional Parquet encryption
- SHA256 checksum verification before deletion
- Graceful shutdown on SIGINT/SIGTERM
- Configurable logging verbosity
- Retention-based pruning
- Parallel processing
- Server-side cursor streaming

### Security

- SQL injection prevention using parameterized identifiers
- S3 server-side encryption (SSE-S3, SSE-KMS)
- Optional client-side Parquet encryption

[0.4.0]: https://github.com/hemashoe/backparq/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/hemashoe/backparq/compare/v0.2.1...v0.3.0
[0.2.1]: https://github.com/hemashoe/backparq/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/hemashoe/backparq/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/hemashoe/backparq/releases/tag/v0.1.0
