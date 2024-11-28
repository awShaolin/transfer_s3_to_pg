import os
import re
import logging
import pandas as pd
import polars as pl
from io import StringIO
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
from libs.s3.s3_handler import S3Handler
from libs.postgresql.postgresql import get_connection_postgres

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def process_table_files(bucket, table_name, date_start, date_end):

    s3 = S3Handler()

    pg_engine = get_connection_postgres()
    try:
        with pg_engine.connect() as connection:
            connection.execute("SELECT 1")
        logging.info(">>> Connection to PostgreSQL successful!")
    except SQLAlchemyError as e:
        logging.error(f">>> Error connecting to PostgreSQL: {str(e)}")
        raise

    s3_file_prefix = f"dbo.{table_name}_"
    formated_table_name = camel_to_snake(table_name)
    schema_name = bucket

    logging.info(f">>> Start transfer from S3 files - {table_name} to PostgreSQL table - {formated_table_name}.")

    # Получение списка файлов из S3
    all_files = s3.list_objects_v2(Bucket=bucket, Prefix=s3_file_prefix)["Contents"]
    date_start_s3 = datetime.strptime(date_start, "%Y-%m-%d").strftime("%Y%m%d")
    date_end_s3 = datetime.strptime(date_end, "%Y-%m-%d").strftime("%Y%m%d")
    filtered_files = [
        f["Key"] for f in all_files
        if date_start_s3 <= f["Key"].split("_")[-1].split(".")[0] <= date_end_s3
    ]

    logging.info(f">>> Found {len(filtered_files)} files in S3 for the period from {date_start} to {date_end}.")
    
    dataframes = []
    row_cnt = 0
    for s3_key in filtered_files:
        obj = s3.get_object(Bucket=bucket, Key=s3_key)
        file_timestamp = s3_key.split("_")[-1].split(".")[0]

        df = pl.read_csv(obj["Body"], separator=";", infer_schema=False)
        df = df.with_columns(pl.lit(file_timestamp).alias("s3_file_timestamp"))
        df = df.with_columns([pl.col(col).cast(pl.Utf8) for col in df.columns])
        df = df.rename({col: camel_to_snake(col) for col in df.columns})

        dataframes.append(df)
        row_cnt += df.shape[0]

    logging.info(f">>> Formed {len(dataframes)} dataframes from S3 files.")
    logging.info(f">>> Total rows before concatenation: {row_cnt}.")

    if not dataframes:
        logging.info(">>> Empty files were received from S3, stop transfer process.")
        return 
    
    combined_df = (
        pl.concat(dataframes)
        .sort(by=["s3_file_timestamp"], descending=False)  
        .unique(subset=["id"], keep="last")
    )

    logging.info(f">>> Total rows after concatenation and deduplication: {len(combined_df)}.")

    with pg_engine.connect() as conn:
        table_exists = conn.execute(
            text(f"SELECT to_regclass('{schema_name}.{formated_table_name}') IS NOT NULL;")
        ).scalar()
        if not table_exists:
            ddl = f"""
                CREATE TABLE {schema_name}.{formated_table_name} (
                    {', '.join([f"{col} TEXT" for col in combined_df.columns])}
                );
            """
            logging.info(f">>> Creating table {schema_name}.{formated_table_name} with DDL: {ddl}")
            conn.execute(text(ddl))

    with pg_engine.begin() as transaction:
        try:
            load_to_pg(pg_engine, schema_name, formated_table_name, combined_df)
        except Exception as e:
            logging.error(f"Error during COPY to {schema_name}.{formated_table_name}: {e}")
            raise

def load_to_pg(engine, schema_name, table_name, df):
    batch_size = 100000

    with engine.connect() as conn:
        ids_to_delete = tuple(df["id"].unique().to_list())
        if ids_to_delete:
            delete_query = text(f"DELETE FROM {schema_name}.{table_name} WHERE id IN :ids;")
            conn.execute(delete_query, {"ids": ids_to_delete})

        for i in range(0, len(df), batch_size):
            batch_df = df.slice(i, batch_size)
            copy_data_to_postgres(conn, schema_name, table_name, batch_df)

def copy_data_to_postgres(conn, schema_name, table_name, df):
    buffer = StringIO()
    df.write_csv(buffer, separator="\t", null_value="\\N")
    buffer.seek(0)

    copy_query = f"""
        COPY {schema_name}.{table_name} ({', '.join(df.columns)}) 
        FROM STDIN WITH (FORMAT CSV, DELIMITER '\t', NULL '\\N');
    """
    try:
        conn.connection.cursor().copy_expert(copy_query, buffer)
        logging.info(f">>> Batch of {len(df)} rows copied to {schema_name}.{table_name}.")
    except Exception as e:
        logging.error(f"Error during COPY to {schema_name}.{table_name}: {e}")
        raise



def camel_to_snake(name):
    s1 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', name)
    return s1.lower()