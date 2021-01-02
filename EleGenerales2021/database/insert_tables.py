import crud as cd
import json
import datetime

con = cd.connect_postgres()
cur = con.cursor()

def insertExpedientesLista():
  print(f'START inserting to organizacion_politica_region at:{datetime.datetime.now()}')

  con = cd.connect_postgres()
  cur = con.cursor()


  with open('../currentRawData/GetExpedientesLista.json','r', encoding='utf-8' ) as jsonfile:
      doc = jsonfile.read()
      arrayCandidatos = json.loads(str(doc))
      count = 0
      cur.execute("TRUNCATE public.organizacion_politica_region;")
      for row in arrayCandidatos:
          cur.execute( 
            "INSERT INTO public.organizacion_politica_region(idOrganizacionPolitica,idExpediente,idJuradoUbicacion,idSolicitudLista,idTipoEleccion,strCarpeta,strCodExpediente,strDistritoElec,strEstadoLista,strJuradoElectoral,strOrganizacionPolitica,strRegion,strTipoOrganizacion,strUbigeo,idPlanGobierno,idProcesoElectoral)\
              VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s,%s, %s,%s)", (
              row["idOrganizacionPolitica"],
              row["idExpediente"],
              row["idJuradoUbicacion"],
              row["idSolicitudLista"],
              row["idTipoEleccion"],
              row["strCarpeta"],
              row["strCodExpediente"],
              row["strDistritoElec"],
              row["strEstadoLista"],
              row["strJuradoElectoral"],
              row["strOrganizacionPolitica"],
              row["strRegion"],
              row["strTipoOrganizacion"],
              row["strUbigeo"],
              row["idPlanGobierno"],
              row["idProcesoElectoral"]))
          count+=1
          # print("insert row organizacion_politica_region",count," success!")
      con.commit()

  print(f'END inserting to organizacion_politica_region at:{datetime.datetime.now()}')



def insertGetCandidatos():
  print(f'START inserting to candidato_info_electoral at:{datetime.datetime.now()}')

  con = cd.connect_postgres()
  cur = con.cursor()


  with open('../currentRawData/GetCandidatos.json','r', encoding='utf-8' ) as jsonfile:
      doc = jsonfile.read()
      arrayCandidatos = json.loads(str(doc))
      count = 0
      cur.execute("TRUNCATE public.candidato_info_electoral;")

      for row in arrayCandidatos:
          cur.execute(
            "INSERT INTO \
              public.candidato_info_electoral( \
                idCandidato, \
                strDocumentoIdentidad, \
                idHojaVida,\
                idSolicitudLista,\
                intPosicion,\
                idCargoEleccion,\
                idExpediente,\
                idEstado,\
                strCargoEleccion,\
                strCandidato,\
                strOrganizacionPolitica,\
                idOrganizacionPolitica,\
                strUbigeoPostula,\
                strUbiDistritoPostula,\
                strUbiProvinciaPostula,\
                strUbiRegionPostula,\
                strEstadoExp,\
                idProcesoElectoral)\
              VALUES( %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s,%s, %s,%s, %s, %s,%s, %s)", (
              row["idCandidato"],
              row["strDocumentoIdentidad"],
              row["idHojaVida"],
              row["idSolicitudLista"],
              row["intPosicion"],
              row["idCargoEleccion"],
              row["idExpediente"],
              row["idEstado"],
              row["strCargoEleccion"],
              row["strCandidato"],
              row["strOrganizacionPolitica"],
              row["idOrganizacionPolitica"],
              row["strUbigeoPostula"],
              row["strUbiDistritoPostula"],
              row["strUbiProvinciaPostula"],
              row["strUbiRegionPostula"],
              row["strEstadoExp"],
              row["idProcesoElectoral"]))
          count+=1
          # con.commit()
          # print("insert row candidato_info_electoral",count," success!")
      con.commit()
  print(f'END inserting to candidato_info_electoral at:{datetime.datetime.now()}')





def insertInfoPersonal():
  print(f'START inserting to candidato_info_personal at:{datetime.datetime.now()}')

  con = cd.connect_postgres()
  cur = con.cursor()
  with open('../currentRawData/CandidatoDatosHV.json','r', encoding='utf-8' ) as jsonfile:
      doc = jsonfile.read()
      arrayCandidatos = json.loads(str(doc))
      count = 0

      cur.execute("TRUNCATE public.candidato_info_personal;")
      for row in arrayCandidatos:
          row = row["oDatosPersonales"]
          cur.execute( \
            "INSERT INTO public.candidato_info_personal(strDocumentoIdentidad,idCandidato,idCargoEleccion,idEstado,idHojaVida,idOrganizacionPolitica,idParamHojaVida,idProcesoElectoral,idTipoEleccion,strAnioPostula,strApellidoMaterno,strApellidoPaterno,strCargoEleccion,strCarneExtranjeria,strClase,strDomiDepartamento,strDomiDistrito,strDomiProvincia,strDomicilioDirecc,strEstado,strFeTerminoRegistro,strFechaNacimiento,strHojaVida,strNaciDepartamento,strNaciDistrito,strNaciProvincia,strNombres,strPaisNacimiento,strPostulaDepartamento,strPostulaDistrito,strPostulaProvincia,strProcesoElectoral,strRutaArchivo,strSexo,strUbigeoDomicilio,strUbigeoNacimiento,strUbigeoPostula,strUsuario)\
              VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s)", (
              row["strDocumentoIdentidad"],
              row["idCandidato"],
              row["idCargoEleccion"],
              row["idEstado"],
              row["idHojaVida"],
              row["idOrganizacionPolitica"],
              row["idParamHojaVida"],
              row["idProcesoElectoral"],
              row["idTipoEleccion"],
              row["strAnioPostula"],
              row["strApellidoMaterno"],
              row["strApellidoPaterno"],
              row["strCargoEleccion"],
              row["strCarneExtranjeria"],
              row["strClase"],
              row["strDomiDepartamento"],
              row["strDomiDistrito"],
              row["strDomiProvincia"],
              row["strDomicilioDirecc"],
              row["strEstado"],
              row["strFeTerminoRegistro"],
              row["strFechaNacimiento"],
              row["strHojaVida"],
              row["strNaciDepartamento"],
              row["strNaciDistrito"],
              row["strNaciProvincia"],
              row["strNombres"],
              row["strPaisNacimiento"],
              row["strPostulaDepartamento"],
              row["strPostulaDistrito"],
              row["strPostulaProvincia"],
              row["strProcesoElectoral"],
              row["strRutaArchivo"],
              row["strSexo"],
              row["strUbigeoDomicilio"],
              row["strUbigeoNacimiento"],
              row["strUbigeoPostula"],
              row["strUsuario"]))
          count+=1
          # con.commit()
          # print("insert row candidato_info_personal",count," success!")
      con.commit()

  print(f'END inserting to candidato_info_personal at:{datetime.datetime.now()}')



def insertExpLab():
  print(f'START inserting to candidato_exp_laboral at:{datetime.datetime.now()}')

  con = cd.connect_postgres()
  cur = con.cursor()
  with open('../currentRawData/CandidatoDatosHV.json','r', encoding='utf-8' ) as jsonfile:
      doc = jsonfile.read()
      arrayCandidatos = json.loads(str(doc))
      count = 0
      cur.execute("TRUNCATE public.candidato_exp_laboral;")

      for objExp in arrayCandidatos:
        arrayExpLab = objExp["lExperienciaLaboral"]
        for row in arrayExpLab:
          cur.execute( \
            "INSERT INTO public.candidato_exp_laboral(idEstado,idHojaVida,idHVExpeLaboral,intItemExpeLaboral,strAnioTrabajoDesde,strAnioTrabajoHasta,strCentroTrabajo,strDireccionTrabajo,strOcupacionProfesion,strRucTrabajo,strTengoExpeLaboral,strTrabajoDepartamento,strTrabajoDistrito,strTrabajoPais,strTrabajoProvincia,strUbigeoTrabajo,strUsuario)\
              VALUES(%s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s)", (
              row["idEstado"],
              row["idHojaVida"],
              row["idHVExpeLaboral"],
              row["intItemExpeLaboral"],
              row["strAnioTrabajoDesde"],
              row["strAnioTrabajoHasta"],
              row["strCentroTrabajo"],
              row["strDireccionTrabajo"],
              row["strOcupacionProfesion"],
              row["strRucTrabajo"],
              row["strTengoExpeLaboral"],
              row["strTrabajoDepartamento"],
              row["strTrabajoDistrito"],
              row["strTrabajoPais"],
              row["strTrabajoProvincia"],
              row["strUbigeoTrabajo"],
              row["strUsuario"]))
          count+=1
          # con.commit()
          # print("insert row candidato_exp_laboral",count," success!")
      con.commit()
  print(f'END inserting to candidato_exp_laboral at:{datetime.datetime.now()}')


def insertEduPostGrado():
  print(f'START inserting to candidato_post_grado at:{datetime.datetime.now()}')

  con = cd.connect_postgres()
  cur = con.cursor()
  with open('../currentRawData/CandidatoDatosHV.json','r', encoding='utf-8' ) as jsonfile:
      doc = jsonfile.read()
      arrayCandidatos = json.loads(str(doc))
      count = 0
      cur.execute("TRUNCATE public.candidato_post_grado;")

      for row in arrayCandidatos:
          row = row["oEduPosgrago"]
          cur.execute( \
            "INSERT INTO public.candidato_post_grado(idEstado,idHojaVida,idHVPosgrado,intItemPosgrado,strAnioPosgrado,strCenEstudioPosgrado,strConcluidoPosgrado,strEgresadoPosgrado,strEsDoctor,strEsMaestro,strEspecialidadPosgrado,strTengoPosgrado,strUsuario)\
              VALUES(%s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s)", (
              row["idEstado"],
              row["idHojaVida"],
              row["idHVPosgrado"],
              row["intItemPosgrado"],
              row["strAnioPosgrado"],
              row["strCenEstudioPosgrado"],
              row["strConcluidoPosgrado"],
              row["strEgresadoPosgrado"],
              row["strEsDoctor"],
              row["strEsMaestro"],
              row["strEspecialidadPosgrado"],
              row["strTengoPosgrado"],
              row["strUsuario"]))
          count+=1
          # con.commit()
          # print("insert row candidato_post_grado",count," success!")
      con.commit()
  print(f'END inserting to candidato_post_grado at:{datetime.datetime.now()}')




def insertEduUni():
  print(f'START inserting to candidato_edu_uni at:{datetime.datetime.now()}')

  con = cd.connect_postgres()
  cur = con.cursor()
  with open('../currentRawData/CandidatoDatosHV.json','r', encoding='utf-8' ) as jsonfile:
      doc = jsonfile.read()
      arrayCandidatos = json.loads(str(doc))
      count = 0
      cur.execute("TRUNCATE public.candidato_edu_uni;")

      for obj in arrayCandidatos:
        objPosGrado = obj["oEduPosgrago"]
        arrayData = obj["lEduUniversitaria"]
        for row in arrayData:
          cur.execute( \
            "INSERT INTO public.candidato_edu_uni(idEstado,idHojaVida,idHVEduUniversitaria,intItemEduUni,strAnioBachiller,strAnioTitulo,strBachillerEduUni,strCarreraUni,strConcluidoEduUni,strEduUniversitaria,strEgresadoEduUni,strMetodoAccion,strOrder,strTengoEduUniversitaria,strTituloUni,strUniversidad,strUsuario)\
              VALUES( %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s)", (
              row["idEstado"],
              objPosGrado["idHojaVida"],
              row["idHVEduUniversitaria"],
              row["intItemEduUni"],
              row["strAnioBachiller"],
              row["strAnioTitulo"],
              row["strBachillerEduUni"],
              row["strCarreraUni"],
              row["strConcluidoEduUni"],
              row["strEduUniversitaria"],
              row["strEgresadoEduUni"],
              row["strMetodoAccion"],
              row["strOrder"],
              row["strTengoEduUniversitaria"],
              row["strTituloUni"],
              row["strUniversidad"],
              row["strUsuario"]))
          count+=1
          # con.commit()
          # print("insert row candidato_edu_uni",count," success!")
      con.commit()
  print(f'END inserting to candidato_edu_uni at:{datetime.datetime.now()}')

def insertEduTecnica():
  print(f'START inserting to candidato_edu_tecnica at:{datetime.datetime.now()}')

  con = cd.connect_postgres()
  cur = con.cursor()
  with open('../currentRawData/CandidatoDatosHV.json','r', encoding='utf-8' ) as jsonfile:
      doc = jsonfile.read()
      arrayCandidatos = json.loads(str(doc))
      count = 0
      cur.execute("TRUNCATE public.candidato_edu_tecnica;")

      for row in arrayCandidatos:
          row = row["oEduTecnico"]
          if row:
            cur.execute( \
              "INSERT INTO public.candidato_edu_tecnica(idEstado,idHojaVida,idHVEduTecnico,strCarreraTecnico,strCenEstudioTecnico,strConcluidoEduTecnico,strTengoEduTecnico,strUsuario)\
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s)", (
                row["idEstado"],
                row["idHojaVida"],
                row["idHVEduTecnico"],
                row["strCarreraTecnico"],
                row["strCenEstudioTecnico"],
                row["strConcluidoEduTecnico"],
                row["strTengoEduTecnico"],
                row["strUsuario"]
                ))
            count+=1
            # con.commit()
            # print("insert row candidato_edu_tecnica",count," success!")
      con.commit()
  print(f'END inserting to candidato_edu_tecnica at:{datetime.datetime.now()}')





def insertEduNoUni():
  print(f'START inserting to candidato_edu_no_uni at:{datetime.datetime.now()}')

  con = cd.connect_postgres()
  cur = con.cursor()
  with open('../currentRawData/CandidatoDatosHV.json','r', encoding='utf-8' ) as jsonfile:
      doc = jsonfile.read()
      arrayCandidatos = json.loads(str(doc))
      count = 0
      cur.execute("TRUNCATE public.candidato_edu_no_uni;")
      for row in arrayCandidatos:
          row = row["oEduNoUniversitaria"]
          cur.execute( \
            "INSERT INTO public.candidato_edu_no_uni(idEstado,idHVNoUniversitaria,idHojaVida,strCarreraNoUni,strCentroEstudioNoUni,strConcluidoNoUni,strTengoNoUniversitaria,strUsuario)\
              VALUES(%s, %s, %s, %s, %s, %s, %s, %s)", (
              row["idEstado"],
              row["idHVNoUniversitaria"],
              row["idHojaVida"],
              row["strCarreraNoUni"],
              row["strCentroEstudioNoUni"],
              row["strConcluidoNoUni"],
              row["strTengoNoUniversitaria"],
              row["strUsuario"]
              ))
          count+=1
          # con.commit()
          # print("insert row candidato_edu_no_uni",count," success!")
      con.commit()

  print(f'END inserting to candidato_edu_no_uni at:{datetime.datetime.now()}')


def insertEduBasic():
  print(f'START inserting to candidato_edu_basic at:{datetime.datetime.now()}')

  con = cd.connect_postgres()
  cur = con.cursor()
  with open('../currentRawData/CandidatoDatosHV.json','r', encoding='utf-8' ) as jsonfile:
      doc = jsonfile.read()
      arrayCandidatos = json.loads(str(doc))
      count = 0
      cur.execute("TRUNCATE public.candidato_edu_basic;")

      for row in arrayCandidatos:
          row = row["oEduBasica"]
          cur.execute( \
            "INSERT INTO public.candidato_edu_basic(idEstado,idHVEduBasica,idHojaVida,strConcluidoEduPrimaria,strConcluidoEduSecundaria,strEduPrimaria,strEduSecundaria,strTengoEduBasica,strUsuario)\
              VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)", (
              row["idEstado"],
              row["idHVEduBasica"],
              row["idHojaVida"],
              row["strConcluidoEduPrimaria"],
              row["strConcluidoEduSecundaria"],
              row["strEduPrimaria"],
              row["strEduSecundaria"],
              row["strTengoEduBasica"],
              row["strUsuario"]
              ))
          count+=1
          # con.commit()
          # print("insert row candidato_edu_basic",count," success!")
      con.commit()
  print(f'END inserting to candidato_edu_basic at:{datetime.datetime.now()}')



def insertSentPenal():

  print(f'END inserting to candidato_sent_penal at:{datetime.datetime.now()}')

  con = cd.connect_postgres()
  cur = con.cursor()
  with open('../currentRawData/CandidatoDatosHV.json','r', encoding='utf-8' ) as jsonfile:
      doc = jsonfile.read()
      arrayCandidatos = json.loads(str(doc))
      count = 0
      cur.execute("TRUNCATE public.candidato_sent_penal;")

      for obj in arrayCandidatos:
        listSentPenal = obj["lSentenciaPenal"]
        for row in listSentPenal:
          cur.execute(
            "INSERT INTO public.candidato_sent_penal(idEstado,idHVSentenciaPenal,idHojaVida,idParamCumpleFallo,idParamModalidad,intItemSentenciaPenal,strCumpleFallo,strDelitoPenal,strExpedientePenal,strFalloPenal,strFechaSentenciaPenal,strModalidad,strOrder,strOrganoJudiPenal,strOtraModalidad,strTengoSentenciaPenal,strUsuario)\
              VALUES(%s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s)", (
              row["idEstado"],
              row["idHVSentenciaPenal"],
              row["idHojaVida"],
              row["idParamCumpleFallo"],
              row["idParamModalidad"],
              row["intItemSentenciaPenal"],
              row["strCumpleFallo"],
              row["strDelitoPenal"],
              row["strExpedientePenal"],
              row["strFalloPenal"],
              row["strFechaSentenciaPenal"],
              row["strModalidad"],
              row["strOrder"],
              row["strOrganoJudiPenal"],
              row["strOtraModalidad"],
              row["strTengoSentenciaPenal"],
              row["strUsuario"]
              ))
          count+=1
          # con.commit()
          # print("insert row candidato_sent_penal",count," success!")
      con.commit()

  print(f'END inserting to candidato_sent_penal at:{datetime.datetime.now()}')



def insertSentCivil():
  print(f'START inserting to candidato_sent_civil at:{datetime.datetime.now()}')

  con = cd.connect_postgres()
  cur = con.cursor()
  with open('../currentRawData/CandidatoDatosHV.json','r', encoding='utf-8' ) as jsonfile:
      doc = jsonfile.read()
      arrayCandidatos = json.loads(str(doc))
      count = 0
      cur.execute("TRUNCATE public.candidato_sent_civil;")

      for obj in arrayCandidatos:
        listSentObl = obj["lSentenciaObliga"]
        for row in listSentObl:
          cur.execute( \
            "INSERT INTO public.candidato_sent_civil(idEstado,idHVSentenciaObliga,idHojaVida,idParamMateriaSentencia,intItemSentenciaObliga,strEstado,strExpedienteObliga,strFalloObliga,strMateriaSentencia,strOrder,strOrganoJuridicialObliga,strTengoSentenciaObliga,strUsuario)\
              VALUES( %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s)", (
              row["idEstado"],
              row["idHVSentenciaObliga"],
              row["idHojaVida"],
              row["idParamMateriaSentencia"],
              row["intItemSentenciaObliga"],
              row["strEstado"],
              row["strExpedienteObliga"],
              row["strFalloObliga"],
              row["strMateriaSentencia"],
              row["strOrder"],
              row["strOrganoJuridicialObliga"],
              row["strTengoSentenciaObliga"],
              row["strUsuario"]
              ))
          count+=1
          # con.commit()
          # print("insert row candidato_sent_civil",count," success!")
      con.commit()
  print(f'END inserting to candidato_sent_civil at:{datetime.datetime.now()}')

def insertCargoPartidario():
  print(f'START inserting to candidato_cargo_partidario at:{datetime.datetime.now()}')

  con = cd.connect_postgres()
  cur = con.cursor()
  with open('../currentRawData/CandidatoDatosHV.json','r', encoding='utf-8' ) as jsonfile:
      doc = jsonfile.read()
      arrayCandidatos = json.loads(str(doc))
      count = 0
      cur.execute("TRUNCATE public.candidato_cargo_partidario;")

      for obj in arrayCandidatos:
        listCargoPart = obj["lCargoPartidario"]
        for row in listCargoPart:
          cur.execute( \
            "INSERT INTO public.candidato_cargo_partidario(idEstado,idHVCargoPartidario,idHojaVida,idOrgPolCargoPartidario,intItemCargoPartidario,strAnioCargoPartiDesde,strAnioCargoPartiHasta,strCargoPartidario,strOrder,strOrgPolCargoPartidario,strTengoCargoPartidario)\
              VALUES(%s, %s, %s, %s, %s, %s, %s, %s,%s, %s,%s)", (
              row["idEstado"],
              row["idHVCargoPartidario"],
              row["idHojaVida"],
              row["idOrgPolCargoPartidario"],
              row["intItemCargoPartidario"],
              row["strAnioCargoPartiDesde"],
              row["strAnioCargoPartiHasta"],
              row["strCargoPartidario"],
              row["strOrder"],
              row["strOrgPolCargoPartidario"],
              row["strTengoCargoPartidario"]
              ))
          count+=1
          # con.commit()
          # print("insert row candidato_cargo_partidario",count," success!")
      con.commit()

  print(f'END inserting to candidato_cargo_partidario at:{datetime.datetime.now()}')




def insertCargoEleccion():
  print(f'START inserting to candidato_cargo_eleccion at:{datetime.datetime.now()}')

  con = cd.connect_postgres()
  cur = con.cursor()
  with open('../currentRawData/CandidatoDatosHV.json','r', encoding='utf-8' ) as jsonfile:
      doc = jsonfile.read()
      arrayCandidatos = json.loads(str(doc))
      count = 0
      cur.execute("TRUNCATE public.candidato_cargo_eleccion;")

      for obj in arrayCandidatos:
        listCargoElecc = obj["lCargoEleccion"]
        for row in listCargoElecc:
          cur.execute( \
            "INSERT INTO public.candidato_cargo_eleccion(idCargoEleccion,idEstado,idHVCargoEleccion,idHojaVida,idOrgPolCargoElec,intItemCargoEleccion,strAnioCargoElecDesde,strAnioCargoElecHasta,strCargoEleccion,strCargoEleccion2,strOrder,strOrgPolCargoElec)\
              VALUES(%s, %s, %s, %s, %s, %s, %s, %s,%s, %s,%s,%s)", (
              row["idCargoEleccion"],
              row["idEstado"],
              row["idHVCargoEleccion"],
              row["idHojaVida"],
              row["idOrgPolCargoElec"],
              row["intItemCargoEleccion"],
              row["strAnioCargoElecDesde"],
              row["strAnioCargoElecHasta"],
              row["strCargoEleccion"],
              row["strCargoEleccion2"],
              row["strOrder"],
              row["strOrgPolCargoElec"]
              ))
          count+=1
          # con.commit()
          # print("insert row candidato_cargo_eleccion",count," success!")
      con.commit()
  print(f'END inserting to candidato_cargo_eleccion at:{datetime.datetime.now()}')





# with open('../currentRawData/CandidatoDatosHV.json','r', encoding='utf-8' ) as jsonfile:
#     doc = jsonfile.read()
#     arrayCandidatos = json.loads(str(doc))
#     count = 0
#     for obj in arrayCandidatos:
#       listCargoElecc = obj["oInfoAdicional"]
#       for row in listCargoElecc:
#         cur.execute( \
#           "INSERT INTO public.candidato_info_adicional(idEstado,idHVInfoAdicional,idHojaVida,strInfoAdicional,strTengoInfoAdicional,strUsuario)\
#             VALUES(%s, %s, %s, %s, %s, %s)", (
#             row["idEstado"],
#             row["idHVInfoAdicional"],
#             row["idHojaVida"],
#             row["strInfoAdicional"],
#             row["strTengoInfoAdicional"],
#             row["strUsuario"]
#             ))
#         count+=1
#         con.commit()
#         print("insert row ",count," success!")
#     con.commit()


# with open('../currentRawData/ProcesosElectorales.json','r', encoding='utf-8' ) as jsonfile:
#     doc = jsonfile.read()
#     arrayCandidatos = json.loads(str(doc))
#     arrayCandidatos2 = arrayCandidatos["data"]
#     count = 0
#     for row in arrayCandidatos2:
#       cur.execute( \
#         "INSERT INTO public.procesos_electorales(idEstado,idProcesoElectoral,intCantidadJee,strDocConvocatoria,strEstado,strFechaAperturaProceso,strFechaConvocatoria,strFechaCierreProceso,strFechaRegistro,strNombreArchivo,strProcesoElectoral,strSiglas,strTipoProceso,strUsuario)\
#           VALUES(%s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s)", (
#           row["idEstado"],
#           row["idProcesoElectoral"],
#           row["intCantidadJee"],
#           row["strDocConvocatoria"],
#           row["strEstado"],
#           row["strFechaAperturaProceso"],
#           row["strFechaConvocatoria"],
#           row["strFechaCierreProceso"],
#           row["strFechaRegistro"],
#           row["strNombreArchivo"],
#           row["strProcesoElectoral"],
#           row["strSiglas"],
#           row["strTipoProceso"],
#           row["strUsuario"]
#           ))
#       count+=1
#       con.commit()
#       print("insert row ",count," success!")
#     con.commit()


# arrayOrg = [21, 2840, 179, 22 , 47, 1264, 2191,  5, 15, 32 ,4 , 1257, 2731, 2646, 2218, 2160, 2173, 14,  55, 1366, 2857]
# count = 1
# for row in arrayOrg:
#   print(row)
#   cur.execute(
#   f"UPDATE public._organizacion_politica \
#   SET ruta_archivo= 'https://aplicaciones007.public.gob.pe/srop_publico/Consulta/Simbolo/GetSimbolo/%20{row}' \
#   WHERE id={count};"
#   )
#   count += 1
#   con.commit()
#   print("insert row ", count, " success!")
# con.commit()
