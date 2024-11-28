import os
import re
import logging
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
from libs.s3.s3_handler import S3Handler
from libs.postgresql.postgresql import get_connection_postgres

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def process_table_files(bucket, table_name, date_start, date_end):

    s3 = S3Handler()

    # check pg connetion 
    pg_engine = get_connection_postgres()
    try:
        with pg_engine.connect() as connection:
            connection.execute("SELECT 1")
        print(">>> Connection to postgresql successful!")
    except SQLAlchemyError as e:
        error_message = str(e.__dict__['orig'])
        print(f">>> Error connecting to PostgreSQL: {error_message}")
        raise
    except Exception as e:
        print(f">>> An unexpected error occurred: {str(e)}")
        raise
    
    s3_file_prefix = f"dbo.{table_name}_"
    formated_table_name = camel_to_snake(table_name)
    schema_name = bucket

    logging.info(f">>> Start transfer from S3 files - {table_name} to PostgreSQL table - {formated_table_name}.")
    
    # get list of files from s3
    all_files = s3.list_objects_v2(Bucket=bucket, Prefix=s3_file_prefix)["Contents"]
    date_start_s3 = datetime.strptime(date_start, "%Y-%m-%d").strftime("%Y%m%d")
    date_end_s3 = datetime.strptime(date_end, "%Y-%m-%d").strftime("%Y%m%d")
    filtered_files = [
        f["Key"] for f in all_files
        if date_start_s3 <= f["Key"].split("_")[-1].split(".")[0] <= date_end_s3
    ]

    logging.info(f">>> Found {len(filtered_files)} files in S3 for the period from {date_start} to {date_end}.")

    # load files to df
    dataframes = []
    row_cnt = 0
    for s3_key in filtered_files:
        obj = s3.get_object(Bucket=bucket, Key=s3_key)
        df = pd.read_csv(obj["Body"], delimiter=';')

        file_timestamp = s3_key.split("_")[-1].split(".")[0]
        df["s3_file_timestamp"] = file_timestamp

        df.columns = [camel_to_snake(col) for col in df.columns]

        df = df.where(pd.notnull(df), None)
        df = df.applymap(lambda x: None if pd.isna(x) else x)

        dataframes.append(df)
        row_cnt += len(df)

    logging.info(f">>> Formed {len(dataframes)} dataframes from S3 files.")
    logging.info(f">>> Total rows before concatenation: {row_cnt}.")

    if not dataframes:
        logging.info(">>> Empty files were received from S3, stop transfer process.")
        return 
    
    combined_df = (
        pd.concat(dataframes)
        .sort_values(by=["s3_file_timestamp"], ascending=True)  
        .drop_duplicates(subset=["id"], keep="last") 
    )

    logging.info(f">>> Total rows after concatenation and deduplication: {len(combined_df)}.")

    # check postgres table 
    with pg_engine.connect() as conn:
        logging.info(">>> Checking if table exists in PostgreSQL...")
        table_exists = conn.execute(
            text(f"SELECT to_regclass('{schema_name}.{formated_table_name}') IS NOT NULL;")
        ).scalar()
        logging.info(f">>> Table exists: {table_exists}")

        if not table_exists:
            ddl = generate_ddl(schema_name, formated_table_name, combined_df)
            conn.execute(text(ddl))

    with pg_engine.begin() as transaction:
        try:
            perform_upsert(pg_engine, schema_name, formated_table_name, combined_df)
        except Exception as e:
            logging.error(f"Error during upsert into {formated_table_name}: {e}")
            transaction.rollback()  
            raise

def generate_ddl(schema_name, table_name, df):
    columns = []

    dtype_mapping = {
        'int64': 'BIGINT',
        'float64': 'DOUBLE PRECISION',
        'object': 'TEXT',
        'bool': 'BOOLEAN',
        'datetime64[ns]': 'TIMESTAMP',
    }

    for col, dtype in df.dtypes.items():
        pg_type = dtype_mapping.get(str(dtype), 'TEXT')
        columns.append(f"{col} {pg_type}")

    if "id" not in df.columns:
        raise ValueError("Column 'id' is required for primary key but is missing in DataFrame.")

    columns.append("PRIMARY KEY (id)")

    ddl = f"CREATE TABLE {schema_name}.{table_name} ({', '.join(columns)});"
    logging.info("Create table with ddl: ")
    logging.info(ddl)
    return ddl


def perform_upsert(engine, schema_name, table_name, df):
    insert_cnt = 0
    update_cnt = 0

    with engine.connect() as conn:
        for _, row in df.iterrows():
            row_dict = row.to_dict()
            columns = ', '.join(row_dict.keys())
            placeholders = ', '.join([f":{key}" for key in row_dict.keys()])
            updates = ', '.join([f"{key} = EXCLUDED.{key}" for key in row_dict.keys()])

            upsert_query = text(f"""
                INSERT INTO {schema_name}.{table_name} ({columns})
                VALUES ({placeholders})
                ON CONFLICT (id) DO UPDATE
                SET {updates}
                RETURNING (xmax = 0) AS is_inserted;
            """)

            try:
                result = conn.execute(upsert_query, row_dict).fetchone()
                if result and result["is_inserted"]:
                    insert_cnt += 1
                else:
                    update_cnt += 1
            except Exception as e:
                logging.error(f">>> Error upserting row with id={row_dict['id']} into {schema_name}.{table_name}: {e}")
                raise

    logging.info(f">>> Upsert completed for table {table_name}: {insert_cnt} rows inserted, {update_cnt} rows updated, total rows processed - {insert_cnt + update_cnt}.")

def camel_to_snake(name):
    s1 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', name)
    return s1.lower()