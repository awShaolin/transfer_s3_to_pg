import os
import re
import logging
import pandas as pd
import polars as pl
from io import StringIO
from sqlalchemy import text
from datetime import datetime
from sqlalchemy.exc import SQLAlchemyError
from s3.s3_handler import S3Handler
# from postgresql.postgresql import get_connection_postgres

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def list_s3_files(s3, bucket, table_name, date_start, date_end):
    prefix = f"dbo.{table_name}_"
    all_files = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)["Contents"]

    date_start_s3 = datetime.strptime(date_start, "%Y-%m-%d").strftime("%Y%m%d")
    date_end_s3 = datetime.strptime(date_end, "%Y-%m-%d").strftime("%Y%m%d")

    filtered_files = [
        f for f in all_files
        if date_start_s3 <= f["Key"].split("_")[-1].split(".")[0] <= date_end_s3
    ]

    logging.info(f">>> Found {len(filtered_files)} files in S3 for the period from {date_start} to {date_end} for table: {table_name}.")

    total_size = sum(f["Size"] for f in filtered_files)
    logging.info(f">>> Total size of filtered files: {total_size / (1024 ** 2):.2f} MB.")

    batch_mode = total_size > 2 * 1024 ** 3
    logging.info(f">>> Will batch load files: {batch_mode}.")

    filtered_files.sort(key=lambda f: extract_timestamp(f["Key"]))

    return filtered_files, batch_mode

# helper utils

def extract_timestamp(filename):
    match = re.search(r"_(\d{14})\d*", filename)
    if match:
        return match.group(1)
    return ""


def process_table_files(bucket, table_name, date_start, date_end):
    s3 = S3Handler()
    
    files, batch_mode = list_s3_files(s3, bucket, table_name, date_start, date_end)
    
    for file in files:
        print(file["Key"])


process_table_files('systech', 'dhGoodsReturn', '2024-03-01', '2024-05-01')