import psycopg2
import os
from os.path import join, dirname
from dotenv import load_dotenv

load_dotenv('../.env')
def connect_db_target():
    return psycopg2.connect(database=os.getenv("DB_NAME_FINAL"), user=os.getenv("DB_USER_FINAL"), \
        password=os.getenv("DB_PASS_FINAL"), host=os.getenv("DB_HOST_FINAL"), port="5432")

def connect_db_origin():
    return psycopg2.connect(database=os.getenv("DB_NAME"), user=os.getenv("DB_USER"), \
        password=os.getenv("DB_PASS"), host=os.getenv("DB_HOST"), port="5432")