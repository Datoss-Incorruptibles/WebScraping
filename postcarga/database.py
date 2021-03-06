import psycopg2
import os
from os.path import join, dirname
from dotenv import load_dotenv

load_dotenv('../.env')

def connect_db():
    return psycopg2.connect(database=os.getenv("DB_NAME_FINAL"), user=os.getenv("DB_USER_FINAL"), \
        password=os.getenv("DB_PASS_FINAL"), host=os.getenv("DB_HOST_FINAL"), port=os.getenv("DB_PORT_FINAL"))
