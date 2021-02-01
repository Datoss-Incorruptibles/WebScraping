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
            truncate table organizacion_politica CASCADE;
            truncate table ubigeo;
            truncate table estudio;
            truncate table institucion;
            truncate table candidato_estudio;
            truncate table candidato_experiencia;
            truncate table candidato_judicial;
            truncate table candidato cASCADE;
            truncate table indicador CASCADE;
            truncate table candidato_mueble;
            truncate table candidato_inmueble;
            truncate table candidato_ingreso;
            ALTER SEQUENCE auth_group_id_seq                       RESTART WITH 1;
            ALTER SEQUENCE auth_group_permissions_id_seq           RESTART WITH 1;
            ALTER SEQUENCE auth_permission_id_seq                  RESTART WITH 1;
            ALTER SEQUENCE auth_user_groups_id_seq                 RESTART WITH 1;
            ALTER SEQUENCE auth_user_id_seq                        RESTART WITH 1;
            ALTER SEQUENCE auth_user_user_permissions_id_seq       RESTART WITH 1;
            ALTER SEQUENCE candidato_estudio_id_seq                RESTART WITH 1;
            ALTER SEQUENCE candidato_experiencia_id_seq            RESTART WITH 1;
            ALTER SEQUENCE candidato_id_seq                        RESTART WITH 1;
            ALTER SEQUENCE candidato_judicial_id_seq               RESTART WITH 1;
            ALTER SEQUENCE cargo_id_seq                            RESTART WITH 1;
            ALTER SEQUENCE django_admin_log_id_seq                 RESTART WITH 1;
            ALTER SEQUENCE django_content_type_id_seq              RESTART WITH 1;
            ALTER SEQUENCE django_migrations_id_seq                RESTART WITH 1;
            ALTER SEQUENCE estudio_id_seq                          RESTART WITH 1;
            ALTER SEQUENCE indicador_categoria_candidato_id_seq    RESTART WITH 1;
            ALTER SEQUENCE indicador_categoria_id_seq              RESTART WITH 1;
            ALTER SEQUENCE indicador_categoria_organizacion_id_seq RESTART WITH 1;
            ALTER SEQUENCE indicador_id_seq                        RESTART WITH 1;
            ALTER SEQUENCE institucion_id_seq                      RESTART WITH 1;
            ALTER SEQUENCE organizacion_politica_id_seq            RESTART WITH 1;
            ALTER SEQUENCE proceso_id_seq                          RESTART WITH 1;
            ALTER SEQUENCE candidato_ingreso_id_seq                RESTART WITH 1;
            ALTER SEQUENCE candidato_mueble_id_seq                RESTART WITH 1;
            ALTER SEQUENCE candidato_inmueble_id_seq                RESTART WITH 1;


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

def add_default_date_value():
    try: 
        con = connect_db()
        cur = con.cursor()
        query = """
            alter table proceso alter column fecha_registro set default now();
            alter table cargo alter column fecha_registro set default now();
            alter table organizacion_politica alter column fecha_registro set default now();
            alter table ubigeo alter column fecha_registro set default now();
            alter table estudio alter column fecha_registro set default now();
            alter table institucion alter column fecha_registro set default now();
            alter table candidato alter column fecha_registro set default now();
            alter table candidato_estudio alter column fecha_registro set default now();
            alter table candidato_judicial alter column fecha_registro set default now();
            alter table candidato_experiencia alter column fecha_registro set default now();
            alter table indicador alter column fecha_registro set default now();
            alter table indicador_categoria alter column fecha_registro set default now();
            alter table indicador_categoria_organizacion alter column fecha_registro set default now();
            alter table indicador_categoria_candidato alter column fecha_registro set default now();
            alter table candidato_ingreso alter column fecha_registro set default now();
            alter table candidato_inmueble alter column fecha_registro set default now();
            alter table candidato_mueble alter column fecha_registro set default now();

        """
        cur.execute(query)
        con.commit()
        con.close()
        print("All fecha_registro change default now() success!")
    except (Exception, psycopg2.Error) as error :
        print ("Error while fetching data", error)

    finally:
        if(con):
            cur.close()
            con.close()