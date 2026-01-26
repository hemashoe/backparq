import datetime as dt
from collections import defaultdict
from typing import NamedTuple

from backparq.config import BackparqConfig
from backparq.s3 import s3_client_from_config
from backparq.db import add_months, month_floor

def prune_backups(config: BackparqConfig, dry_run: bool = False) -> None:
    if not config.archive.retention.enabled:
        print("Retention policy is disabled in config.")
        return

    if not config.s3.bucket:
        print("No S3 bucket configured.")
        return

    s3 = s3_client_from_config(config.s3)
    bucket = config.s3.bucket
    prefix = config.s3.prefix
    
    to_delete = []

    if config.archive.mode == "backup":
        # Mode: Backup (Snapshots)
        # Strategy: Keep latest N snapshots (days/months roughly maps to counts or time matching?)
        # User request: "retent on created time of backup last 12 or whatever"
        # Since we use daily/scheduled runs, let's assume 'days' = keep last N runs? 
        # Or time-based? "older than X days" is better.
        
        days = config.archive.retention.days
        months = config.archive.retention.months
        
        # Determine cutoff time
        # Any backup created BEFORE this time is deleted.
        now = dt.datetime.now(dt.timezone.utc)
        cutoff_date = now
        
        if days > 0:
            cutoff_date = now - dt.timedelta(days=days)
            print(f"Pruning BACKUPS created older than {days} days (Before: {cutoff_date})")
        elif months > 0:
            cutoff_date = now - dt.timedelta(days=months * 30) # approx
            print(f"Pruning BACKUPS created older than {months} months (Before: {cutoff_date})")
        else:
             print("Retention enabled but no days/months set.")
             return

        # List "backups/" folder
        # Prefix: {prefix}/backups/{RUN_ID}/...
        # RUN_ID is "YYYY-MM-DD_HHMMSS"
        
        backup_root = f"{prefix}/backups/"
        print(f"Scanning {backup_root}...")
        
        paginator = s3.get_paginator("list_objects_v2")
        result = s3.list_objects_v2(Bucket=bucket, Prefix=backup_root, Delimiter="/")
        
        # 'CommonPrefixes' contains the run folders
        runs = []
        for p in result.get("CommonPrefixes", []):
            # p['Prefix'] = "prefix/backups/2025-01-01_120000/"
            folder = p["Prefix"]
            run_id = folder.rstrip("/").split("/")[-1]
            try:
                run_time = dt.datetime.strptime(run_id, "%Y-%m-%d_%H%M%S").replace(tzinfo=dt.timezone.utc)
                runs.append((run_time, folder))
            except ValueError:
                continue

        for run_time, folder in runs:
            if run_time < cutoff_date:
                # Delete this entire folder
                # We need to list all objects recursively under this folder
                print(f"Marking obsolete backup: {run_id} (Date: {run_time})")
                
                # Recursively list objects to delete
                for page in paginator.paginate(Bucket=bucket, Prefix=folder):
                    for obj in page.get("Contents", []):
                        to_delete.append({"Key": obj["Key"]})

    else:
        # Mode: Offload (Archive)
        # Strategy: Data Age (Chunk Date)
        # Existing logic
        days = config.archive.retention.days
        months = config.archive.retention.months
        
        now = dt.datetime.now(dt.timezone.utc)
        cutoff_date = now
        
        if days > 0:
            cutoff_date = now - dt.timedelta(days=days)
            print(f"Pruning ARCHIVED DATA older than {days} days (Data Date < {cutoff_date.date()})")
        elif months > 0:
            year = now.year - (months // 12)
            month = now.month - (months % 12)
            if month <= 0:
                month += 12
                year -= 1
            cutoff_date = dt.datetime(year, month, 1, tzinfo=dt.timezone.utc)
            print(f"Pruning ARCHIVED DATA older than {months} months (Data Date < {year}-{month:02})")

        print(f"Scanning s3://{bucket}/{prefix}/archive/ ...")
        paginator = s3.get_paginator("list_objects_v2")
        
        # Scan archive folder
        archive_root = f"{prefix}/archive/"
        
        for page in paginator.paginate(Bucket=bucket, Prefix=archive_root):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                # Parse: .../year=YYYY/month=MM/...
                parts = key.split("/")
                year = 0
                month = 0
                for p in parts:
                    if p.startswith("year="):
                         try: year = int(p.split("=")[1])
                         except: pass
                    if p.startswith("month="):
                         try: month = int(p.split("=")[1])
                         except: pass
                
                if year > 0 and month > 0:
                    chunk_date = dt.datetime(year, month, 1, tzinfo=dt.timezone.utc)
                    if chunk_date < cutoff_date:
                        to_delete.append({"Key": key})

    if not to_delete:
        print("No files to prune.")
        return

    print(f"Found {len(to_delete)} files to prune.")
    
    if dry_run:
        print("DRY RUN: Files that would be deleted:")
        for d in to_delete[:5]:
            print(f" - {d['Key']}")
        if len(to_delete) > 5: print(" ... and more")
        return

    # Delete in batches
    batch_size = 1000
    deleted_count = 0
    from tqdm import tqdm
    
    for i in tqdm(range(0, len(to_delete), batch_size), desc="Pruning"):
        batch = to_delete[i:i+batch_size]
        try:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": batch})
            deleted_count += len(batch)
        except Exception as e:
            print(f"Error deleting batch: {e}")
            
    print(f"Successfully pruned {deleted_count} files.")
