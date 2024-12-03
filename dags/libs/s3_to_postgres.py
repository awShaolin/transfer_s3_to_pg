import io
import os
import re
import shutil
import logging
import polars as pl
import pandas as pd
from sqlalchemy import text
from datetime import datetime, timezone
from libs.s3.s3_handler import S3Handler
from libs.postgresql.postgresql import get_connection_postgres

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def list_s3_files(s3_handler, s3_bucket, s3_prefix, date_start, date_end):
    try:
        logging.info(f">>> List files in S3 bucket: {s3_bucket} with prefix: {s3_prefix}")
        files = []
        response = s3_handler.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)

        date_start = datetime.strptime(date_start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        date_end = datetime.strptime(date_end, "%Y-%m-%d").replace(tzinfo=timezone.utc)

        for obj in response.get("Contents", []):
            last_modified = obj["LastModified"]

            if date_start <= last_modified <= date_end:
                files.append({
                    "file_path": obj["Key"],
                    "file_size": obj["Size"],
                    "file_date": last_modified
                })

        sorted_files = sorted(files, key=lambda x: x["file_date"])
        logging.info(f">>> Found {len(sorted_files)} files in S3 bucket.")
        return sorted_files
    
    except Exception as e:
        logging.error(f">>> Error while listing S3 files: {e}")
        raise 

def process_single_file(s3_handler, s3_bucket, file_path):
    try:
        logging.info(f">>> Reading file from S3: {file_path}")
        response = s3_handler.get_object(Bucket=s3_bucket, Key=file_path)
        content = response["Body"].read().decode('utf-8')

        df = pd.read_csv(io.StringIO(content), sep=";", dtype=str)
        logging.info(f">>> Successfully read file {file_path} into DataFrame.")

        if df.empty:
            return None

        return preprocess_df(df, file_path)
    
    except Exception as e:
        logging.error(f">>> Error processing single file {file_path}: {e}")
        raise

def process_large_file(s3, s3_bucket, file_path, pg_engine, schema_name, table_name, local_dwnld_base, table_checked, batch_size):
        
    logging.info(f">>> Processing large file: {file_path}")

    local_dwnld_dir = os.path.join(local_dwnld_base, table_name)
    os.makedirs(local_dwnld_dir, exist_ok=True)
    local_file_path = os.path.join(local_dwnld_dir, os.path.basename(file_path))

    try:
        logging.info(f">>> Downloading file {file_path} from S3 to {local_file_path}.")
        with open(local_file_path, "wb") as f:
            s3.download_fileobj(s3_bucket, file_path, f)
            logging.info(f">>> Successfully downlod file {file_path} to local dir.")

        reader = pd.read_csv(
            local_file_path,
            sep=";",
            chunksize=batch_size
        )  

        for idx, chunk in enumerate(reader):
            df = pd.DataFrame(chunk)
            logging.info(f">>> Load {idx} chunk to DF.")

            processed_df = preprocess_df(df, file_path)

            if not table_checked:
                logging.info(
                    f">>> Existence of PostgreSQL table: {schema_name}.{table_name} is not checked. Start checking..."
                )
                check_postgres_table(pg_engine, schema_name, table_name, processed_df)
                table_checked = True

            insert_data_to_postgres(pg_engine, schema_name, table_name, processed_df)
            logging.info(f">>> Successfully processed and uploaded chunk {idx} from file {file_path}.")

    except Exception as e:
        logging.error(f">>> Error processing file {file_path}: {e}")
        raise
    finally:
        logging.info(f">>> Cleaning up local files for table {table_name}.")
        shutil.rmtree(local_dwnld_dir)
        logging.info(f">>> Completed processing large file {file_path}.")

def preprocess_df(df, file_path):
    try:
        logging.info(">>> Preprocessing DataFrame...")
        timestamp = file_path.split("_")[-1].split(".")[0]
        df["s3_date"] = timestamp

        df.columns = [to_snake_case(col) for col in df.columns]

        df = df.sort_values("s3_date").drop_duplicates(subset=["id"], keep="first")
        
        logging.info(">>> Successfully preprocessed DataFrame.")

        return df
    
    except Exception as e:
        logging.error(f">>> Error preprocessing DataFrame from file {file_path}: {e}")
        raise

def check_postgres_table(pg_engine, schema_name, table_name, df):
    try:
        with pg_engine.connect() as conn:
            logging.info(">>> Checking if table exists in PostgreSQL...")
            table_exists = conn.execute(
                text(f"SELECT to_regclass('{schema_name}.{table_name}') IS NOT NULL;")
            ).scalar()
            logging.info(f">>> Table exists: {table_exists}")

            if not table_exists:
                logging.info(f"Table {schema_name}.{table_name} does not exist.")
                logging.info(f"Starting create table process...")

                columns = ", ".join([f"{col.lower()} text" for col in df.columns])
                ddl = f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} ({columns});"

                logging.info(f"Table {schema_name}.{table_name} will be created with following ddl: ")
                logging.info(f"{ddl}")

                conn.execute(ddl)
            else:
                logging.info(f">>> Table {schema_name}.{table_name} exists.")
    except Exception as e: 
        logging.error(f">>> Error checking/creating PostgreSQL table {schema_name}.{table_name}: {e}")
        raise

def insert_data_to_postgres(pg_engine, schema_name, table_name, df):
    logging.info(f">>> Inserting data to postgres table - {schema_name}.{table_name}.")

    ids_in_df = set(df["id"].to_list())
    ids_in_df_str = ','.join(map(str, ids_in_df))

    with pg_engine.connect() as conn:
        query = f"""
                    SELECT id, s3_date 
                    FROM {schema_name}.{table_name} 
                    WHERE id IN ({', '.join(f"'{id_value}'" for id_value in ids_in_df)})
                """
        ids_in_pg = conn.execute(query).fetchall()
        ids_in_pg = {row[0]: row[1] for row in ids_in_pg}

        logging.info(f">>> Will be checked {len(ids_in_pg)} id's in PostgreSQL and DataFrame.")

        to_delete_in_pg = []
        to_delete_in_df = []

        for _, row in df.iterrows():
            row_id = row["id"]
            row_date = row["s3_date"]
            if row_id in ids_in_pg:
                if row_date > ids_in_pg[row_id]:
                    to_delete_in_pg.append(row_id)
                else:
                    to_delete_in_df.append(row_id)

        logging.info(f">>> {len(to_delete_in_pg)} rows will be deleted from {schema_name}.{table_name}.")
        logging.info(f">>> {len(to_delete_in_df)} rows will be deleted from the DataFrame.")

        if to_delete_in_pg:
            conn.execute(f"DELETE FROM {schema_name}.{table_name} WHERE id IN ({', '.join(f"'{id_value}'" for id_value in ids_in_df)})")

        df_filtered = df[~df["id"].isin(to_delete_in_df)]

        logging.info(">>> Suggested rows were deleted from {schema_name}.{table_name} and DataFrame.")
        logging.info(f"{len(df_filtered)} rows will be inserted to PostgreSQL.")
        try:
            df_filtered.to_sql(table_name, conn, schema=schema_name, if_exists="append", index=False, chunksize=50000)
            logging.info(f">>> Successfully insert {len(df_filtered)} rows to {schema_name}.{table_name}.")
        except Exception as e:
            logging.error(f">>> Error inserting data into PostgreSQL table {schema_name}.{table_name}: {e}")
            raise

def to_snake_case(name):
    return re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()

def process_table_files(s3_bucket, table_name, date_start=None, date_end=None):
    S3_PREFIX = f"dbo.{table_name}_"
    BATCH_FILE_SIZE = 100 * 1024 * 1024
    BATCH_ROW_SIZE = 50000
    PG_SCHEMA = 'systech'
    PG_TABLE = to_snake_case(table_name)
    LOCAL_DWNLD_BASE = '/tmp/systech'

    if os.path.exists(LOCAL_DWNLD_BASE):
        logging.info(f"Cleaning up local download dir {LOCAL_DWNLD_BASE}.")
        shutil.rmtree(LOCAL_DWNLD_BASE)

    if not date_start or not date_end:
        logging.warning(f">>> start_date and end_date is None. Stop transfering data from S3 for {table_name}")
        return

    s3 = S3Handler()
    pg_engine = get_connection_postgres()

    logging.info(f">>> Start transfer process for {table_name}")

    s3_files = list_s3_files(s3, s3_bucket, S3_PREFIX, date_start, date_end)

    table_checked = False
    for file in s3_files:
        try:
            file_path = file["file_path"]
            file_size = file["file_size"]
            file_size_mb = file_size / (1024 * 1024)

            logging.info(f">>> Start transfering file {file_path} from s3.")
            if file_size < BATCH_FILE_SIZE:
                logging.info(f"File size is {file_size_mb:.2f} MB => will be processed as a single chunk.")
                df = process_single_file(s3, s3_bucket, file_path)

                if df is None: 
                    logging.info(f">>> File {file_path} is empty. Skipping this file.")
                    continue

                if not table_checked:
                    logging.info(f">>> Existence of PostgreSQL table: {PG_SCHEMA}.{PG_TABLE} is not checked. Start checking...")
                    check_postgres_table(pg_engine, PG_SCHEMA, PG_TABLE, df)
                    table_checked = True
                
                insert_data_to_postgres(pg_engine, PG_SCHEMA, PG_TABLE, df)
            else:
                logging.info(f">>> File size is {file_size_mb:.2f} MB => will be processed as multiple chunks.")
                process_large_file(s3, s3_bucket, file_path, pg_engine, PG_SCHEMA, PG_TABLE, LOCAL_DWNLD_BASE, table_checked, BATCH_ROW_SIZE)
        except Exception as e:
            logging.error(f">>> Error processing file {file['file_path']}: {e}")
            raise
    logging.info(f">>> Completed processing for table {table_name}.")
    