import psycopg2
import os
from os.path import join, dirname
from dotenv import load_dotenv

load_dotenv('../../.env')

con = psycopg2.connect(database=os.getenv("DB_NAME"), user=os.getenv("DB_USER"), password=os.getenv("DB_PASS"), host=os.getenv("DB_HOST"), port="5432")
print(con)
print("Database opened successfully")

# create tables 

cur = con.cursor()
cur.execute('''
DROP TABLE IF EXISTS  public.organizacion_politica_region;
CREATE TABLE IF NOT EXISTS public.organizacion_politica_region(
    idOrganizacionPolitica int,
    idExpediente int,
    idJuradoUbicacion int,
    idSolicitudLista int,
    idTipoEleccion int,
    strCarpeta character varying,
    strCodExpediente character varying,
    strDistritoElec character varying,
    strEstadoLista character varying,
    strJuradoElectoral character varying,
    strOrganizacionPolitica character varying,
    strRegion character varying,
    strTipoOrganizacion character varying,
    strUbigeo character varying,
    idPlanGobierno int,
    idProcesoElectoral int,
    fecha_registro TIMESTAMP DEFAULT now() NOT NULL
    );
''')
con.commit()
print("Table organizacion_politica_region created successfully")




cur = con.cursor()
cur.execute('''
DROP TABLE IF EXISTS  public.candidato_info_electoral;

CREATE TABLE IF NOT EXISTS public.candidato_info_electoral (
    idCandidato int,
    strDocumentoIdentidad character varying,
    idHojaVida int,
    idSolicitudLista  int,
    intPosicion int,
    idCargoEleccion int,
    idExpediente int,
    idEstado int,
    strCargoEleccion character varying,
    strCandidato character varying,
    strOrganizacionPolitica character varying,
    idOrganizacionPolitica int,
    strUbigeoPostula character varying,
    strUbiDistritoPostula character varying,
    strUbiProvinciaPostula character varying,
    strUbiRegionPostula character varying,
    strEstadoExp character varying,
    idProcesoElectoral int,
    fecha_registro TIMESTAMP DEFAULT now() NOT NULL
    );
''')
con.commit()
print("Table candidato_info_electoral created successfully")











cur = con.cursor()
cur.execute('''
DROP TABLE IF EXISTS  public.candidato_info_personal;

CREATE TABLE IF NOT EXISTS public.candidato_info_personal (
    strDocumentoIdentidad character varying,
    idCandidato int,
    idCargoEleccion int,
    idEstado int,
    idHojaVida int,
    idOrganizacionPolitica int,
    idParamHojaVida int,
    idProcesoElectoral int,
    idTipoEleccion int,
    strAnioPostula character varying,
    strApellidoMaterno character varying,
    strApellidoPaterno character varying,
    strCargoEleccion character varying,
    strCarneExtranjeria character varying,
    strClase character varying,
    strDomiDepartamento character varying,
    strDomiDistrito character varying,
    strDomiProvincia character varying,
    strDomicilioDirecc character varying,
    strEstado character varying,
    strFeTerminoRegistro character varying,
    strFechaNacimiento character varying,
    strHojaVida character varying,
    strNaciDepartamento character varying,
    strNaciDistrito character varying,
    strNaciProvincia character varying,
    strNombres character varying,
    strPaisNacimiento character varying,
    strPostulaDepartamento character varying,
    strPostulaDistrito character varying,
    strPostulaProvincia character varying,
    strProcesoElectoral character varying,
    strRutaArchivo character varying,
    strSexo character varying,
    strUbigeoDomicilio character varying,
    strUbigeoNacimiento character varying,
    strUbigeoPostula character varying,
    strUsuario character varying,
    fecha_registro TIMESTAMP DEFAULT now() NOT NULL
    );
''')
con.commit()
print("Table candidato_info_personal created successfully")






cur = con.cursor()
cur.execute('''
DROP TABLE IF EXISTS  public.candidato_exp_laboral;

CREATE TABLE IF NOT EXISTS public.candidato_exp_laboral (
    idEstado int,
    idHojaVida int,
    idHVExpeLaboral int,
    intItemExpeLaboral int,
    strAnioTrabajoDesde character varying,
    strAnioTrabajoHasta character varying,
    strCentroTrabajo character varying,
    strDireccionTrabajo character varying,
    strOcupacionProfesion character varying,
    strRucTrabajo character varying,
    strTengoExpeLaboral character varying,
    strTrabajoDepartamento character varying,
    strTrabajoDistrito character varying,
    strTrabajoPais character varying,
    strTrabajoProvincia character varying,
    strUbigeoTrabajo character varying,
    strUsuario character varying,
    fecha_registro TIMESTAMP DEFAULT now() NOT NULL
    );
''')
con.commit()
print("Table candidato_exp_laboral created successfully")




# drop table candidato_post_laboral;
# drop table candidato_post_grado;

# drop table candidato_post_grado;

cur = con.cursor()
cur.execute('''
DROP TABLE IF EXISTS  public.candidato_post_grado;

CREATE TABLE IF NOT EXISTS public.candidato_post_grado (
    idEstado int,
    idHojaVida int,
    idHVPosgrado int,
    intItemPosgrado int,
    strAnioPosgrado character varying,
    strCenEstudioPosgrado character varying,
    strConcluidoPosgrado character varying,
    strEgresadoPosgrado character varying,
    strEsDoctor character varying,
    strEsMaestro character varying,
    strEspecialidadPosgrado character varying,
    strTengoPosgrado character varying,
    strUsuario character varying,
    fecha_registro TIMESTAMP DEFAULT now() NOT NULL
    );
''')
con.commit()
print("Table candidato_post_grado created successfully")



cur = con.cursor()
cur.execute('''
DROP TABLE IF EXISTS  public.candidato_edu_uni;

CREATE TABLE IF NOT EXISTS public.candidato_edu_uni(
    idEstado int,
    idHojaVida int,
    idHVEduUniversitaria int,
    intItemEduUni int,
    strAnioBachiller character varying,
    strAnioTitulo character varying,
    strBachillerEduUni character varying,
    strCarreraUni character varying,
    strConcluidoEduUni character varying,
    strEduUniversitaria character varying,
    strEgresadoEduUni character varying,
    strMetodoAccion character varying,
    strOrder character varying,
    strTengoEduUniversitaria character varying,
    strTituloUni character varying,
    strUniversidad character varying,
    strUsuario character varying,
    fecha_registro TIMESTAMP DEFAULT now() NOT NULL
    );
''')
con.commit()
print("Table candidato_edu_uni created successfully")





cur = con.cursor()
cur.execute('''
DROP TABLE IF EXISTS  public.candidato_edu_tecnica;

CREATE TABLE IF NOT EXISTS public.candidato_edu_tecnica(
    idEstado int,
    idHojaVida int,
    idHVEduTecnico int,
    strCarreraTecnico character varying,
    strCenEstudioTecnico character varying,
    strConcluidoEduTecnico character varying,
    strTengoEduTecnico character varying,
    strUsuario character varying,
    fecha_registro TIMESTAMP DEFAULT now() NOT NULL
    );
''')
con.commit()
print("Table candidato_edu_tecnica created successfully")







cur = con.cursor()
cur.execute('''
DROP TABLE IF EXISTS  public.candidato_edu_no_uni;

CREATE TABLE IF NOT EXISTS public.candidato_edu_no_uni(
    idEstado int,
    idHVNoUniversitaria int,
    idHojaVida int,
    strCarreraNoUni character varying,
    strCentroEstudioNoUni character varying,
    strConcluidoNoUni character varying,
    strTengoNoUniversitaria character varying,
    strUsuario character varying,
    fecha_registro TIMESTAMP DEFAULT now() NOT NULL
    );
''')
con.commit()
print("Table candidato_edu_no_uni created successfully")



cur = con.cursor()
cur.execute('''
DROP TABLE IF EXISTS  public.candidato_edu_basic;

CREATE TABLE IF NOT EXISTS public.candidato_edu_basic(
    idEstado int,
    idHVEduBasica int,
    idHojaVida int,
    strConcluidoEduPrimaria character varying,
    strConcluidoEduSecundaria character varying,
    strEduPrimaria character varying,
    strEduSecundaria character varying,
    strTengoEduBasica character varying,
    strUsuario character varying,
    fecha_registro TIMESTAMP DEFAULT now() NOT NULL
    );
''')
con.commit()
print("Table candidato_edu_basic created successfully")





cur = con.cursor()
cur.execute('''
DROP TABLE IF EXISTS  public.candidato_sent_penal;

CREATE TABLE IF NOT EXISTS public.candidato_sent_penal(
    idEstado int,
    idHVSentenciaPenal int,
    idHojaVida int,
    idParamCumpleFallo int,
    idParamModalidad int,
    intItemSentenciaPenal int,
    strCumpleFallo character varying,
    strDelitoPenal character varying,
    strExpedientePenal character varying,
    strFalloPenal character varying,
    strFechaSentenciaPenal character varying,
    strModalidad character varying,
    strOrder character varying,
    strOrganoJudiPenal character varying,
    strOtraModalidad character varying,
    strTengoSentenciaPenal character varying,
    strUsuario character varying,
    fecha_registro TIMESTAMP DEFAULT now() NOT NULL
    );
''')
con.commit()
print("Table candidato_sent_penal created successfully")



cur = con.cursor()
cur.execute('''
DROP TABLE IF EXISTS  public.candidato_sent_civil;

CREATE TABLE IF NOT EXISTS public.candidato_sent_civil(
    idEstado int,
    idHVSentenciaObliga int,
    idHojaVida int,
    idParamMateriaSentencia int,
    intItemSentenciaObliga int,
    strEstado character varying,
    strExpedienteObliga character varying,
    strFalloObliga character varying,
    strMateriaSentencia character varying,
    strOrder character varying,
    strOrganoJuridicialObliga character varying,
    strTengoSentenciaObliga character varying,
    strUsuario character varying,
    fecha_registro TIMESTAMP DEFAULT now() NOT NULL
    );
''')
con.commit()
print("Table candidato_sent_civil created successfully")








cur = con.cursor()
cur.execute('''
DROP TABLE IF EXISTS  public.candidato_cargo_partidario;

CREATE TABLE IF NOT EXISTS public.candidato_cargo_partidario(
    idEstado int,
    idHVCargoPartidario int,
    idHojaVida int,
    idOrgPolCargoPartidario int,
    intItemCargoPartidario int,
    strAnioCargoPartiDesde character varying,
    strAnioCargoPartiHasta character varying,
    strCargoPartidario character varying,
    strOrder character varying,
    strOrgPolCargoPartidario character varying,
    strTengoCargoPartidario character varying,
    fecha_registro TIMESTAMP DEFAULT now() NOT NULL
    );
''')
con.commit()
print("Table candidato_cargo_partidario created successfully")





cur = con.cursor()
cur.execute('''
DROP TABLE IF EXISTS  public.candidato_cargo_eleccion;

CREATE TABLE IF NOT EXISTS public.candidato_cargo_eleccion(
    idCargoEleccion int,
    idEstado int,
    idHVCargoEleccion int,
    idHojaVida int,
    idOrgPolCargoElec int,
    intItemCargoEleccion int,
    strAnioCargoElecDesde character varying,
    strAnioCargoElecHasta character varying,
    strCargoEleccion character varying,
    strCargoEleccion2 character varying,
    strOrder character varying,
    strOrgPolCargoElec character varying,
    fecha_registro TIMESTAMP DEFAULT now() NOT NULL
    );
''')
con.commit()
print("Table candidato_cargo_eleccion created successfully")






# oInfoAdicional

# cur = con.cursor()
# cur.execute('''
# drop table candidato_info_adicional;

# CREATE TABLE IF NOT EXISTS candidato_info_adicional(
#     idEstado int,
#     idHVInfoAdicional int,
#     idHojaVida int,
#     strInfoAdicional character varying,
#     strTengoInfoAdicional character varying,
#     strUsuario character varying
#     );
# ''')
# con.commit()
# print("Table candidato_info_adicional created successfully")






# cur = con.cursor()
# cur.execute('''
# CREATE TABLE IF NOT EXISTS candidato_ingresos(
#     decOtroIngresoPrivado int,
#     decOtroIngresoPublico int,
#     decRemuBrutaPrivado int,
#     decRemuBrutaPublico int,
#     decRentaIndividualPrivado int,
#     decRentaIndividualPublico int,
#     idEstado int,
#     idHVIngresos int,
#     idHojaVida int,
#     strAnioIngresos character varying,
#     strTengoIngresos character varying,
#     strUsuario character varying
#     );
# ''')
# con.commit()
# print("Table candidato_info_adicional created successfully")






# cur = con.cursor()
# cur.execute('''
# CREATE TABLE IF NOT EXISTS procesos_electorales(
#     idEstado int,
#     idProcesoElectoral int,
#     idTipoProceso int,
#     intCantidadJee int,
#     strDocConvocatoria character varying,
#     strEstado character varying,
#     strFechaAperturaProceso character varying,
#     strFechaConvocatoria character varying,
#     strFechaCierreProceso character varying,
#     strFechaRegistro character varying,
#     strNombreArchivo character varying,
#     strProcesoElectoral character varying,
#     strSiglas character varying,
#     strTipoProceso character varying,
#     strUsuario character varying
#     );
# ''')
# con.commit()
# print("Table proceso created successfully")


con.close()

