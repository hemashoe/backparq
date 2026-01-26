import datetime as dt
from collections import defaultdict
from typing import NamedTuple

from backparq.config import BackparqConfig
from backparq.s3 import s3_client_from_config
from backparq.parquet import load_manifest
from backparq.archive import s3_key_for_chunk
from backparq.db import ChunkSpec

class BackupStat(NamedTuple):
    table: str
    year: int
    month: int
    rows: int
    size_bytes: int
    key: str
    verified: bool

def check_backups(config: BackparqConfig, prefix_filter: str = "") -> None:
    """
    List backups in S3 and print summary.
    """
    if not config.s3.bucket:
        print("No S3 bucket configured.")
        return

    s3 = s3_client_from_config(config.s3)
    bucket = config.s3.bucket
    prefix = config.s3.prefix
    if prefix_filter:
        prefix = f"{prefix}/{prefix_filter}".replace("//", "/")
    
    print(f"Checking backups in s3://{bucket}/{prefix} ...")

    paginator = s3.get_paginator("list_objects_v2")
    
    stats_by_table = defaultdict(list)
    total_size = 0
    total_rows = 0
    
    # We scan S3 objects.
    # Structure: prefix/table/year=YYYY/month=MM/name.parquet
    # We look for .parquet files.
    
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".parquet"):
                continue
                
            size = obj["Size"]
            total_size += size
            
            # Parse key to get metadata?
            # Or read object metadata (head_object) for 'sha256'?
            # Doing HeadObject on every file is slow. 
            # We trust the listing for existence.
            
            parts = key.split("/")
            # Attempt to extract table, year, month
            # Format: .../table/year=YYYY/month=MM/file.parquet
            if len(parts) >= 4:
                try:
                    # simplistic parsing
                    table = parts[-4]
                    year_str = parts[-3]
                    month_str = parts[-2]
                    
                    year = int(year_str.split("=")[1]) if "=" in year_str else 0
                    month = int(month_str.split("=")[1]) if "=" in month_str else 0
                    
                    stats_by_table[table].append(BackupStat(
                        table=table,
                        year=year,
                        month=month,
                        rows=-1, # Unknown without manifest or footer
                        size_bytes=size,
                        key=key,
                        verified=True # Exists
                    ))
                except Exception:
                    # Ignore weird files
                    pass

    print(f"\nFound {sum(len(l) for l in stats_by_table.values())} backup files.")
    print(f"Total Size: {total_size / (1024*1024):.2f} MB\n")
    
    for table, stats in stats_by_table.items():
        table_size = sum(s.size_bytes for s in stats)
        min_date = min((s.year, s.month) for s in stats) if stats else ("-", "-")
        max_date = max((s.year, s.month) for s in stats) if stats else ("-", "-")
        
        print(f"Table: {table}")
        print(f"  Files: {len(stats)}")
        print(f"  Size:  {table_size / (1024*1024):.2f} MB")
        print(f"  Range: {min_date[0]}-{min_date[1]:02} to {max_date[0]}-{max_date[1]:02}")
        print("")
