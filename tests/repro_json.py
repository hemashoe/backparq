import pyarrow as pa
from sqlalchemy import create_engine, text
from testcontainers.postgres import PostgresContainer

# We need backparq.db functions to test exact behavior
from backparq.db import insert_arrow_table_to_pg


def test_json_roundtrip():
    with PostgresContainer("postgres:15") as postgres:
        engine = create_engine(postgres.get_connection_url())
        with engine.connect() as conn:
            conn.execute(
                text("""
                CREATE TABLE test_types (
                    id SERIAL PRIMARY KEY,
                    data JSONB,
                    tags TEXT[]
                )
            """)
            )
            conn.commit()

            # Insert via SQL
            conn.execute(
                text("""
                INSERT INTO test_types (data, tags) VALUES
                ('{"a": 1, "b": "hello"}', ARRAY['tag1', 'tag2'])
            """)
            )
            conn.commit()

            # Simulate generic read (like backparq does)
            # psycopg2 returns dict for JSON and list for ARRAY
            res = conn.execute(text("SELECT id, data, tags FROM test_types")).fetchall()
            rows = [dict(row._mapping) for row in res]

            # rows[0]['data'] is {'a': 1, 'b': 'hello'} (dict)
            # rows[0]['tags'] is ['tag1', 'tag2'] (list)

            # Create Arrow Table
            table = pa.Table.from_pylist(rows)

            print(f"Arrow Schema: {table.schema}")
            # Likely: data: struct<a: int64, b: string>, tags: list<item: string>

            # Simulate Restore (Insert back)
            # This calls insert_arrow_table_to_pg which uses CSV COPY
            # This is where we expect failure if formatting is wrong

            # First clean table
            conn.execute(text("TRUNCATE test_types"))
            conn.commit()

        # We need raw psycopg2 connection for our function
        import psycopg2

        raw_conn = psycopg2.connect(
            host=postgres.get_container_host_ip(),
            port=postgres.get_exposed_port(5432),
            dbname=postgres.POSTGRES_DB,
            user=postgres.POSTGRES_USER,
            password=postgres.POSTGRES_PASSWORD,
        )

        try:
            insert_arrow_table_to_pg(raw_conn, "test_types", table, conflict_mode="do_nothing")
            print("Restore successful!")
        except Exception as e:
            print(f"Restore FAILED: {e}")
            raise e
        finally:
            raw_conn.close()


if __name__ == "__main__":
    try:
        test_json_roundtrip()
    except Exception as e:
        print(f"Test failed with: {e}")
        exit(1)
