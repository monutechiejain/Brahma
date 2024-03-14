import logging
import boto3
from botocore.exceptions import ClientError

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy import *
from common.constants import PROFILE_PROPERTIES

class Sqlalchemy(Session):
    session_factory_map = dict()
    session_factory = None
    base = declarative_base()

    def get_db_credentials(cls):
        logging.info('inside get_db_credentials')
        secret_name = "arn:aws:secretsmanager:ap-south-1:899676613710:secret:dbPrd-2ygpom"
        region_name = "ap-south-1"

        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name
        )

        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name
            )
            logging.info(get_secret_value_response)
            return get_secret_value_response['SecretString']
        except ClientError as e:
            logging.error(e)
            raise e

    def __new__(cls, *args, **kwargs):
        SCHEMA_NAME = args[0]

        db_url = 'mysql+mysqlconnector://' + PROFILE_PROPERTIES['DB_USER'] + \
                 ':' + PROFILE_PROPERTIES['DB_PASSWORD'] + \
                 '@' + PROFILE_PROPERTIES['DB_HOST'] + \
                 ':3306/' + \
                 SCHEMA_NAME

        engine = create_engine(db_url, pool_size=20, max_overflow=100)
        session_factory = sessionmaker(bind=engine, autocommit=False)
        cls.base = declarative_base(bind=engine)
        cls.session_factory_map[SCHEMA_NAME] = session_factory()

        return cls.session_factory_map[SCHEMA_NAME]



