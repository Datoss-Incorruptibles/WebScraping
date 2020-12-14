import psycopg2
import os
from os.path import join, dirname
from dotenv import load_dotenv

load_dotenv('../.env')

con = psycopg2.connect(database=os.getenv("DB_NAME"), user=os.getenv("DB_USER"), password=os.getenv("DB_PASS"), host=os.getenv("DB_HOST"), port="5432")
print(con)
print("Database opened successfully")

## create tables 

# cur = con.cursor()
# cur.execute('''
# drop table candidato_info_electoral;
# CREATE TABLE IF NOT EXISTS candidato_info_electoral (
#     idCandidato int,
#     strDocumentoIdentidad character varying,
#     idCargoEleccion int,
#     strCargoEleccion character varying,
#     strCandidato character varying,
#     strOrganizacionPolitica character varying,
#     strTipoOrganizacion character varying,
#     strRegion character varying,
#     strDistritoElec character varying,
#     strUbigeoPostula character varying,
#     idExpediente int,
#     idEstado int,
#     strEstadoExp character varying
#     );
# ''')
# con.commit()
# print("Table candidato_info_electoral created successfully")


# drop table candidato_info_personal;

# cur = con.cursor()
# cur.execute('''
# drop table candidato_exp_laboral;
# CREATE TABLE IF NOT EXISTS candidato_exp_laboral (
#     strDocumentoIdentidad character varying,
#     idEstado int,
#     idHojaVida int,
#     idHVExpeLaboral int,
#     intItemExpeLaboral int,
#     strAnioTrabajoDesde character varying,
#     strAnioTrabajoHasta character varying,
#     strCentroTrabajo character varying,
#     strDireccionTrabajo character varying,
#     strOcupacionProfesion character varying,
#     strRucTrabajo character varying,
#     strTengoExpeLaboral character varying,
#     strTrabajoDepartamento character varying,
#     strTrabajoDistrito character varying,
#     strTrabajoPais character varying,
#     strTrabajoProvincia character varying,
#     strUbigeoTrabajo character varying,
#     strUsuario character varying
#     );
# ''')
# con.commit()
# print("Table candidato_exp_laboral created successfully")




# drop table candidato_post_laboral;
# drop table candidato_post_grado;


# cur = con.cursor()
# cur.execute('''
# CREATE TABLE IF NOT EXISTS candidato_post_grado (
#     strDocumentoIdentidad character varying,
#     idEstado int,
#     idHojaVida int,
#     idHVPosgrado int,
#     intItemPosgrado int,
#     strAnioPosgrado character varying,
#     strCenEstudioPosgrado character varying,
#     strConcluidoPosgrado character varying,
#     strEgresadoPosgrado character varying,
#     strEsDoctor character varying,
#     strEsMaestro character varying,
#     strEspecialidadPosgrado character varying,
#     strTengoPosgrado character varying,
#     strUsuario character varying
#     );
# ''')
# con.commit()
# print("Table candidato_post_grado created successfully")



# cur = con.cursor()
# cur.execute('''
# drop table candidato_edu_uni;
# CREATE TABLE IF NOT EXISTS candidato_edu_uni(
#     strDocumentoIdentidad character varying,
#     idEstado int,
#     idHojaVida int,
#     idHVEduUniversitaria int,
#     intItemEduUni int,
#     strAnioBachiller character varying,
#     strAnioTitulo character varying,
#     strBachillerEduUni character varying,
#     strCarreraUni character varying,
#     strConcluidoEduUni character varying,
#     strEduUniversitaria character varying,
#     strEgresadoEduUni character varying,
#     strMetodoAccion character varying,
#     strOrder character varying,
#     strTengoEduUniversitaria character varying,
#     strTituloUni character varying,
#     strUniversidad character varying,
#     strUsuario character varying
#     );
# ''')
# con.commit()
# print("Table candidato_edu_uni created successfully")

# drop table candidato_edu_tecnica;

# cur = con.cursor()
# cur.execute('''
# CREATE TABLE IF NOT EXISTS candidato_edu_tecnica(
#     strDocumentoIdentidad character varying,
#     idEstado int,
#     idHojaVida int,
#     idHVEduTecnico int,
#     strCarreraTecnico character varying,
#     strCenEstudioTecnico character varying,
#     strConcluidoEduTecnico character varying,
#     strTengoEduTecnico character varying,
#     strUsuario character varying
#     );
# ''')
# con.commit()
# print("Table candidato_edu_uni created successfully")







# cur = con.cursor()
# cur.execute('''
# CREATE TABLE IF NOT EXISTS candidato_edu_no_uni(
#     strDocumentoIdentidad character varying,
#     idEstado int,
#     idHVNoUniversitaria int,
#     idHojaVida int,
#     strCarreraNoUni character varying,
#     strCentroEstudioNoUni character varying,
#     strConcluidoNoUni character varying,
#     strTengoNoUniversitaria character varying,
#     strUsuario character varying
#     );
# ''')
# con.commit()
# print("Table candidato_edu_no_uni created successfully")



# cur = con.cursor()
# cur.execute('''
# CREATE TABLE IF NOT EXISTS candidato_sent_penal(
#     strDocumentoIdentidad character varying,
#     idEstado int,
#     idHVSentenciaPenal int,
#     idHojaVida int,
#     idParamCumpleFallo int,
#     idParamModalidad int,
#     intItemSentenciaPenal int,
#     strCumpleFallo character varying,
#     strDelitoPenal character varying,
#     strExpedientePenal character varying,
#     strFalloPenal character varying,
#     strFechaSentenciaPenal character varying,
#     strModalidad character varying,
#     strOrder character varying,
#     strOrganoJudiPenal character varying,
#     strOtraModalidad character varying,
#     strTengoSentenciaPenal character varying,
#     strUsuario character varying
#     );
# ''')
# con.commit()
# print("Table candidato_sent_penal created successfully")



# cur = con.cursor()
# cur.execute('''
# CREATE TABLE IF NOT EXISTS candidato_sent_civil(
#     strDocumentoIdentidad character varying,
#     idEstado int,
#     idHVSentenciaObliga int,
#     idHojaVida int,
#     idParamMateriaSentencia int,
#     intItemSentenciaObliga int,
#     strEstado character varying,
#     strExpedienteObliga character varying,
#     strFalloObliga character varying,
#     strMateriaSentencia character varying,
#     strOrder character varying,
#     strOrganoJuridicialObliga character varying,
#     strTengoSentenciaObliga character varying,
#     strUsuario character varying
#     );
# ''')
# con.commit()
# print("Table candidato_sent_civil created successfully")






# cur = con.cursor()
# cur.execute('''
# CREATE TABLE IF NOT EXISTS candidato_edu_basic(
#     strDocumentoIdentidad character varying,
#     idEstado int,
#     idHVEduBasica int,
#     idHojaVida int,
#     strConcluidoEduPrimaria character varying,
#     strConcluidoEduSecundaria character varying,
#     strEduPrimaria character varying,
#     strEduSecundaria character varying,
#     strTengoEduBasica character varying,
#     strUsuario character varying
#     );
# ''')
# con.commit()
# print("Table candidato_edu_basic created successfully")


# con.close()


