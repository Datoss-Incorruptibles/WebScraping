import psycopg2
import os
from os.path import join, dirname
from dotenv import load_dotenv

load_dotenv('../.env')

def connect_db():
    return psycopg2.connect(database=os.getenv("DB_NAME_FINAL"), user=os.getenv("DB_USER_FINAL"), \
        password=os.getenv("DB_PASS_FINAL"), host=os.getenv("DB_HOST_FINAL"), port="5432")


def clean_all_data():
    try: 
        con = connect_db()
        cur = con.cursor()
        query = """
            truncate table proceso;
            truncate table cargo;
            truncate table organizacion_politica;
            truncate table ubigeo;
            truncate table estudio;
            truncate table institucion;
            truncate table candidato_estudio;
            truncate table candidato_experiencia;
            truncate table candidato_judicial;
            truncate table candidato;
        """
        cur.execute(query)
        con.commit()
        con.close()
        print("All data cleaned success!")
    except (Exception, psycopg2.Error) as error :
        print ("Error while fetching data", error)

    finally:
        if(con):
            cur.close()
            con.close()