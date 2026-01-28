import datetime as dt

import pytest
from sqlalchemy import create_engine, text
from testcontainers.minio import MinioContainer
from testcontainers.postgres import PostgresContainer

from backparq.archive import archive_tables
from backparq.config import (
    ArchiveConfig,
    BackparqConfig,
    DatabaseConfig,
    ParquetConfig,
    S3Config,
)
from backparq.restore import restore_tables

# Skip if docker not available or libs missing
try:
    import docker

    client = docker.from_env()
    client.ping()
except Exception:
    pytest.skip("Docker not available", allow_module_level=True)


@pytest.fixture(scope="module")
def postgres():
    with PostgresContainer("postgres:15") as postgres:
        yield postgres


@pytest.fixture(scope="module")
def minio():
    with MinioContainer() as minio:
        yield minio


@pytest.fixture
def db_config(postgres):
    return DatabaseConfig(
        host=postgres.get_container_host_ip(),
        port=postgres.get_exposed_port(5432),
        name=postgres.POSTGRES_DB,
        user=postgres.POSTGRES_USER,
        password=postgres.POSTGRES_PASSWORD,
        sslmode="disable",
    )


@pytest.fixture
def s3_config(minio):
    client = minio.get_client()
    bucket = "test-archive"
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    config = minio.get_config()
    return S3Config(
        bucket=bucket,
        endpoint_url=f"http://{config['endpoint']}",
        access_key_id=config["access_key"],
        secret_access_key=config["secret_key"],
        use_ssl=False,
        verify_ssl=False,
        addressing_style="path",  # Minio usually needs path style
    )


def test_archive_and_restore(postgres, db_config, s3_config, tmp_path):
    # 1. Setup Data
    engine = create_engine(postgres.get_connection_url())
    with engine.connect() as conn:
        conn.execute(
            text("""
            CREATE TABLE test_events (
                id SERIAL PRIMARY KEY,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                payload TEXT
            )
        """)
        )
        # Insert data: Jan 2023, Feb 2023
        conn.execute(
            text("""
            INSERT INTO test_events (created_at, payload) VALUES
            ('2023-01-15 10:00:00+00', 'event_jan_1'),
            ('2023-01-20 10:00:00+00', 'event_jan_2'),
            ('2023-02-10 10:00:00+00', 'event_feb_1')
        """)
        )
        conn.commit()

    # 2. Configure Backparq
    from backparq.config import ParquetEncryptionConfig

    config = BackparqConfig(
        database=db_config,
        s3=s3_config,
        parquet=ParquetConfig(encryption=ParquetEncryptionConfig(enabled=False)),
        archive=ArchiveConfig(
            tables=["test_events"],
            cutoff_exclusive=dt.datetime(
                2023, 2, 1, tzinfo=dt.timezone.utc
            ),  # Archive Jan, keep Feb
            base_dir=tmp_path,
            perform_delete=True,
            delete_batch_size=10,
        ),
    )
    # Fix encryption config structure for real object
    # We should probably improve the Config object creation in tests or use a helper

    # 3. Run Archive
    archive_tables(config)

    # 4. Verify Archive Effects
    with engine.connect() as conn:
        # Jan should be gone, Feb should remain
        jan_count = conn.execute(
            text("SELECT count(*) FROM test_events WHERE created_at < '2023-02-01'")
        ).scalar()
        feb_count = conn.execute(
            text("SELECT count(*) FROM test_events WHERE created_at >= '2023-02-01'")
        ).scalar()
        assert jan_count == 0, "Jan data should be deleted"
        assert feb_count == 1, "Feb data should remain"

    # 5. Run Restore
    restore_tables(
        config=config,
        start_date=dt.datetime(2023, 1, 1, tzinfo=dt.timezone.utc),
        end_date=dt.datetime(2023, 2, 1, tzinfo=dt.timezone.utc),
        conflict_mode="do_nothing",
    )

    # 6. Verify Restore
    with engine.connect() as conn:
        jan_count = conn.execute(
            text("SELECT count(*) FROM test_events WHERE created_at < '2023-02-01'")
        ).scalar()
        assert jan_count == 2, "Jan data should be restored"
