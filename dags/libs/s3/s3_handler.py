import logging
import configparser
import boto3
import boto3.session
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

class S3Handler:
    def __init__(self):
        config = configparser.ConfigParser()
        config.read('dags/creds/s3_config.ini') #/home/airflow/airflow/dags/creds/s3_config.ini

        self.s3_access_key_id = config['S3']['S3_KEY_ID']
        self.s3_secret_access_key = config['S3']['S3_SECRET_KEY']
        self.s3_region = config['S3']['S3_REGION']
        self.service_name = config['S3']['S3_SERVICE_NAME']
        self.endpoint_url = config['S3']['S3_ENDPOINT_URL']
        self.s3 = self.get_conn()

    def get_conn(self):
        """
        Get connection to s3
        """
        session = boto3.session.Session(
                aws_access_key_id=self.s3_access_key_id,
                aws_secret_access_key=self.s3_secret_access_key,
                region_name=self.s3_region
            )
        try:
            self.s3 = session.client(
                service_name=self.service_name,
                endpoint_url=self.endpoint_url
            )
            response = self.s3.list_buckets()
            logging.info(f">>> Connection successful to {self.endpoint_url}.")
        except NoCredentialsError:
            logging.error(">>> No credentials found. Ensure your S3 credentials are configured in ~/.aws/credentials or they defined in env.")
            raise
        except PartialCredentialsError:
            logging.error(">>> Incomplete credentials found. Check your S3 configuration.")
            raise
        except Exception as e:
            logging.error(f">>> An error occurred while connecting: {e}")
            raise
        
        return self.s3
        
    def __getattr__(self, name):
        return getattr(self.s3, name)