import psycopg2
import os
from os.path import join, dirname
from dotenv import load_dotenv

load_dotenv('../../.env')
def connect_postgres():
    return psycopg2.connect(database=os.getenv("DB_NAME"), user=os.getenv("DB_USER"), \
        password=os.getenv("DB_PASS"), host=os.getenv("DB_HOST"), port="5432")
