import psycopg2
import os
from os.path import join, dirname
from dotenv import load_dotenv
load_dotenv('../../.env')

con = psycopg2.connect(database=os.getenv("DB_NAME"), user=os.getenv("DB_USER"), password=os.getenv("DB_PASS"), host=os.getenv("DB_HOST"), port="5432")
print(con)
print("Database opened successfully")



## insert db



def createRankingUnis():
    cur = con.cursor()
    cur.execute('''
    drop table IF EXISTS de.ranking_unis;

    CREATE TABLE IF NOT EXISTS de.ranking_unis(
        rankingNum character varying,
        universidadName character varying,
        paisName character varying,
        urlImagePais character varying,
        impactoPosicion character varying,
        aperturaPosicion character varying,
        excelenciaPosicion character varying,
        fecha_registro TIMESTAMP DEFAULT now() NOT NULL
        );
    ''')
    con.commit()
    print("Table ranking_unis created successfully")



def uploadRanking():
  cur = con.cursor()
  cur.execute("TRUNCATE de.ranking_unis;")
  copy_sql = """
            COPY de.ranking_unis FROM stdin WITH CSV HEADER
            DELIMITER as ','
            """

  with open('./rankingUnis.csv', 'r') as f:
    cur.copy_expert(sql=copy_sql, file=f)
    con.commit()
    print("Table rankingUnis cargado successfully")




# createRankingUnis()

# uploadRanking()


def createLicenciamientoSunedu():
    cur = con.cursor()
    cur.execute('''
    drop table IF EXISTS de.linc_sunedu;
    CREATE TABLE IF NOT EXISTS de.linc_sunedu(
        codigoEntidad character varying,
        nombre character varying,
        tipoGestion character varying,
        estado_licenciamiento character varying,
        periodo_licenciamiento  character varying,
        departamento_local character varying,
        provincia_local character varying,
        distrito_local character varying,
        latitud_ubicacion character varying,
        longitud_ubicacion character varying
        );
    ''')
    con.commit()
    print("Table linc_sunedu created successfully")



def uploadLicenciamiento():
  cur = con.cursor()
  cur.execute("TRUNCATE de.linc_sunedu;")
  copy_sql = """
            COPY de.linc_sunedu FROM stdin WITH CSV HEADER
            DELIMITER as ','
            """

  with open('./licenciamiento.csv', 'r') as f:
    cur.copy_expert(sql=copy_sql, file=f)
    con.commit()
    print("Table lincSunedu cargado successfully")



createLicenciamientoSunedu()
uploadLicenciamiento()