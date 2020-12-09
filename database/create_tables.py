import psycopg2
import os
from os.path import join, dirname
from dotenv import load_dotenv

load_dotenv('../.env')

con = psycopg2.connect(database=os.getenv("DB_NAME"), user=os.getenv("DB_USER"), password=os.getenv("DB_PASS"), host=os.getenv("DB_HOST"), port="5432")

print("Database opened successfully")

## create tables 

cur = con.cursor()
cur.execute('''
CREATE TABLE IF NOT EXISTS ranking_universidad (
    rankingNum bigint,
    universidadName character varying,
    presenciaPosicion int,
    impactoPosicion int,
    aperturaPosicion int ,
    excelenciaPosicion int);
''')
print("Table ranking_universidad created successfully")

con.commit()

cur = con.cursor()
cur.execute('''
drop table candidato;
CREATE TABLE IF NOT EXISTS candidato (
    Id_proceso  bigint,
    Id_Candidato bigint,
    DNI character varying,
    Nombre_Completo character varying,
    POS int,
    Cargo character varying,
    Jurisdiccion character varying,
    Designado character varying,
    Estado character varying,
    Nombres character varying,
    APaterno character varying,
    AMaterno character varying,
    Fecha_Nac character varying,
    Id_Sexo int,
    Pais character varying,
    Departamento character varying,
    Provincia character varying,
    Distrito character varying,
    Residencia character varying,
    Correo character varying,
    Registro_Org_Pol character varying,
    Portal_Web character varying,
    Cargo_Autoridad character varying,
    Forma_Designacion character varying);
''')
print("Table candidato created successfully")

con.commit()

cur = con.cursor()
cur.execute('''
drop table candidato_ed_basica;
CREATE TABLE IF NOT EXISTS candidato_ed_basica (
    id_proceso bigint,
    id_candidato bigint,
    dni character varying,
    nombre_completo character varying,
    tipo_educacion int,
    centro_Primaria character varying,
    primaria character varying,
    ubigeo_primaria_departamento character varying,
    ubigeo_primaria_provincia character varying,
    ubigeo_primaria_distrito character varying,
    anio_inicio_primaria int,
    anio_fin_primaria int,
    centro_secundaria character varying,
    secundaria character varying,
    ubigeo_secundaria_departamento character varying,
    ubigeo_secundaria_provincia character varying,
    ubigeo_secundaria_distrito character varying,
    anio_inicio_secundaria int,
    anio_fin_secundaria int,
    pais character varying,
    gradoI int,
    gradoII int,
    gradoIII int,
    gradoIV int,
    gradoV int,
    gradoVI int);
''')

print("Table candidato_ed_basica created successfully")
con.commit()

cur = con.cursor()
cur.execute('''
drop table candidato_ed_superior;
CREATE TABLE IF NOT EXISTS candidato_ed_superior(
    id_proceso bigint,
    id_candidato  bigint,
    dni character varying,
    nombre_completo character varying,
    tipo_estudio int,
    nombre_carrera character varying,
    nombre_estudio character varying,
    nombre_centro character varying,
    concluido boolean,
    tipoGrado character varying,
    otro_tipo_grado character varying,
    anio_inicio int, 
    anio_final int);
''')
print("Table candidato_ed_superior created successfully")
con.commit()



cur = con.cursor()
cur.execute('''
drop table candidato_experiencia;
CREATE TABLE IF NOT EXISTS candidato_experiencia (
    id_proceso bigint, 
    id_candidato bigint,
    dni character varying, 
    nombre_completo character varying, 
    condicion int,
    Empleador character varying,
    Ruc character varying,
    Pais character   varying,
    Nombre_Sector character varying,
    Cargo character varying,
    InicioAnio int,
    FinAnio int);
''')
print("Table candidato_experiencia created successfully")
con.commit()


con.close()


