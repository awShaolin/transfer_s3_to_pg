from airflow.models import Variable
from sqlalchemy import create_engine

# for aifrlow run postgres
db_host = Variable.get("postgres_host")
db_port = Variable.get("postgres_port")
db_name = Variable.get("postgres_database")
db_user = Variable.get("postgres_username")
db_password = Variable.get("postgres_password")


def get_connection_postgres() -> create_engine:
    'creates connection engine to database'

    return create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
