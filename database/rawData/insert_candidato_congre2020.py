import crud as cd
import json

con = cd.connect_postgres()

cur = con.cursor()

# with open('../PlataformaElectoral/EleCongreExtra2020/CandidatoInfoElectoral.json','r', encoding='utf-8' ) as jsonfile:
#     doc = jsonfile.read()
#     arrayCandidatos = json.loads(str(doc))
#     count = 0
#     for row in arrayCandidatos:
#         # print(row)
#         cur.execute( \
#           "INSERT INTO public.candidato_info_electoral(idCandidato,strDocumentoIdentidad,idHojaVida,idSolicitudLista,intPosicion,idCargoEleccion,idExpediente,idEstado,strCargoEleccion,strCandidato,strOrganizacionPolitica,strTipoOrganizacion,strRegion,strDistritoElec,strUbigeoPostula,strEstadoExp,strUbiRegionPostula,idProcesoElectoral)\
#             VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s,%s, %s,%s, %s, %s)", (
#             row["idCandidato"],
#             row["strDocumentoIdentidad"],
#             row["idHojaVida"],
#             row["idSolicitudLista"],
#             row["intPosicion"],
#             row["idCargoEleccion"],
#             row["idExpediente"],
#             row["idEstado"],
#             row["strCargoEleccion"],
#             row["strCandidato"],
#             row["strOrganizacionPolitica"],
#             row["strTipoOrganizacion"],
#             row["strRegion"],
#             row["strDistritoElec"],
#             row["strUbigeoPostula"],
#             row["strEstadoExp"],
#             row["strUbiRegionPostula"],
#             row["idProcesoElectoral"]))
#         count+=1
#         # con.commit()
#         print("insert row ",count," success!")
#     con.commit()





# with open('../PlataformaElectoral/EleCongreExtra2020/CandidatoDatosPersonales.json','r', encoding='utf-8' ) as jsonfile:
#     doc = jsonfile.read()
#     arrayCandidatos = json.loads(str(doc))
#     count = 0
#     for row in arrayCandidatos:
#         # print(row)
#         cur.execute( \
#           "INSERT INTO public.candidato_info_personal(strDocumentoIdentidad,idCandidato,idCargoEleccion,idEstado,idHojaVida,idOrganizacionPolitica,idParamHojaVida,idProcesoElectoral,idTipoEleccion,strAnioPostula,strApellidoMaterno,strApellidoPaterno,strCargoEleccion,strCarneExtranjeria,strClase,strDomiDepartamento,strDomiDistrito,strDomiProvincia,strDomicilioDirecc,strEstado,strFeTerminoRegistro,strFechaNacimiento,strHojaVida,strNaciDepartamento,strNaciDistrito,strNaciProvincia,strNombres,strPaisNacimiento,strPostulaDepartamento,strPostulaDistrito,strPostulaProvincia,strProcesoElectoral,strRutaArchivo,strSexo,strUbigeoDomicilio,strUbigeoNacimiento,strUbigeoPostula,strUsuario)\
#             VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s)", (
#             row["strDocumentoIdentidad"],
#             row["idCandidato"],
#             row["idCargoEleccion"],
#             row["idEstado"],
#             row["idHojaVida"],
#             row["idOrganizacionPolitica"],
#             row["idParamHojaVida"],
#             row["idProcesoElectoral"],
#             row["idTipoEleccion"],
#             row["strAnioPostula"],
#             row["strApellidoMaterno"],
#             row["strApellidoPaterno"],
#             row["strCargoEleccion"],
#             row["strCarneExtranjeria"],
#             row["strClase"],
#             row["strDomiDepartamento"],
#             row["strDomiDistrito"],
#             row["strDomiProvincia"],
#             row["strDomicilioDirecc"],
#             row["strEstado"],
#             row["strFeTerminoRegistro"],
#             row["strFechaNacimiento"],
#             row["strHojaVida"],
#             row["strNaciDepartamento"],
#             row["strNaciDistrito"],
#             row["strNaciProvincia"],
#             row["strNombres"],
#             row["strPaisNacimiento"],
#             row["strPostulaDepartamento"],
#             row["strPostulaDistrito"],
#             row["strPostulaProvincia"],
#             row["strProcesoElectoral"],
#             row["strRutaArchivo"],
#             row["strSexo"],
#             row["strUbigeoDomicilio"],
#             row["strUbigeoNacimiento"],
#             row["strUbigeoPostula"],
#             row["strUsuario"]))
#         count+=1
#         # con.commit()
#         print("insert row ",count," success!")
#     con.commit()


# with open('../PlataformaElectoral/EleCongreExtra2020/CandidatoExpLaboral.json','r', encoding='utf-8' ) as jsonfile:
#     doc = jsonfile.read()
#     arrayCandidatos = json.loads(str(doc))
#     count = 0
#     for row in arrayCandidatos:
#         cur.execute( \
#           "INSERT INTO public.candidato_exp_laboral(strDocumentoIdentidad,idEstado,idHojaVida,idHVExpeLaboral,intItemExpeLaboral,strAnioTrabajoDesde,strAnioTrabajoHasta,strCentroTrabajo,strDireccionTrabajo,strOcupacionProfesion,strRucTrabajo,strTengoExpeLaboral,strTrabajoDepartamento,strTrabajoDistrito,strTrabajoPais,strTrabajoProvincia,strUbigeoTrabajo,strUsuario)\
#             VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s)", (
#             row["strDocumentoIdentidad"],
#             row["idEstado"],
#             row["idHojaVida"],
#             row["idHVExpeLaboral"],
#             row["intItemExpeLaboral"],
#             row["strAnioTrabajoDesde"],
#             row["strAnioTrabajoHasta"],
#             row["strCentroTrabajo"],
#             row["strDireccionTrabajo"],
#             row["strOcupacionProfesion"],
#             row["strRucTrabajo"],
#             row["strTengoExpeLaboral"],
#             row["strTrabajoDepartamento"],
#             row["strTrabajoDistrito"],
#             row["strTrabajoPais"],
#             row["strTrabajoProvincia"],
#             row["strUbigeoTrabajo"],
#             row["strUsuario"]))
#         count+=1
#         # con.commit()
#         print("insert row ",count," success!")
#     con.commit()




# with open('../PlataformaElectoral/EleCongreExtra2020/CandidatoPostGrado.json','r', encoding='utf-8' ) as jsonfile:
#     doc = jsonfile.read()
#     arrayCandidatos = json.loads(str(doc))
#     count = 0
#     for row in arrayCandidatos:
#         cur.execute( \
#           "INSERT INTO public.candidato_post_grado(strDocumentoIdentidad,idEstado,idHojaVida,idHVPosgrado,intItemPosgrado,strAnioPosgrado,strCenEstudioPosgrado,strConcluidoPosgrado,strEgresadoPosgrado,strEsDoctor,strEsMaestro,strEspecialidadPosgrado,strTengoPosgrado,strUsuario)\
#             VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s)", (
#             row["strDocumentoIdentidad"],
#             row["idEstado"],
#             row["idHojaVida"],
#             row["idHVPosgrado"],
#             row["intItemPosgrado"],
#             row["strAnioPosgrado"],
#             row["strCenEstudioPosgrado"],
#             row["strConcluidoPosgrado"],
#             row["strEgresadoPosgrado"],
#             row["strEsDoctor"],
#             row["strEsMaestro"],
#             row["strEspecialidadPosgrado"],
#             row["strTengoPosgrado"],
#             row["strUsuario"]))
#         count+=1
#         # con.commit()
#         print("insert row ",count," success!")
#     con.commit()


# with open('../PlataformaElectoral/EleCongreExtra2020/CandidatoEduUni.json','r', encoding='utf-8' ) as jsonfile:
#     doc = jsonfile.read()
#     arrayCandidatos = json.loads(str(doc))
#     count = 0
#     for row in arrayCandidatos:
#         cur.execute( \
#           "INSERT INTO public.candidato_edu_uni(strDocumentoIdentidad,idEstado,idHojaVida,idHVEduUniversitaria,intItemEduUni,strAnioBachiller,strAnioTitulo,strBachillerEduUni,strCarreraUni,strConcluidoEduUni,strEduUniversitaria,strEgresadoEduUni,strMetodoAccion,strOrder,strTengoEduUniversitaria,strTituloUni,strUniversidad,strUsuario)\
#             VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s)", (
#             row["strDocumentoIdentidad"],
#             row["idEstado"],
#             row["idHojaVida"],
#             row["idHVEduUniversitaria"],
#             row["intItemEduUni"],
#             row["strAnioBachiller"],
#             row["strAnioTitulo"],
#             row["strBachillerEduUni"],
#             row["strCarreraUni"],
#             row["strConcluidoEduUni"],
#             row["strEduUniversitaria"],
#             row["strEgresadoEduUni"],
#             row["strMetodoAccion"],
#             row["strOrder"],
#             row["strTengoEduUniversitaria"],
#             row["strTituloUni"],
#             row["strUniversidad"],
#             row["strUsuario"]))
#         count+=1
#         # con.commit()
#         print("insert row ",count," success!")
#     con.commit()




# with open('../PlataformaElectoral/EleCongreExtra2020/CandidatoEduTecnica.json','r', encoding='utf-8' ) as jsonfile:
#     doc = jsonfile.read()
#     arrayCandidatos = json.loads(str(doc))
#     count = 0
#     for row in arrayCandidatos:
#         cur.execute( \
#           "INSERT INTO public.candidato_edu_tecnica(strDocumentoIdentidad,idEstado,idHojaVida,idHVEduTecnico,strCarreraTecnico,strCenEstudioTecnico,strConcluidoEduTecnico,strTengoEduTecnico,strUsuario)\
#             VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)", (
#             row["strDocumentoIdentidad"],
#             row["idEstado"],
#             row["idHojaVida"],
#             row["idHVEduTecnico"],
#             row["strCarreraTecnico"],
#             row["strCenEstudioTecnico"],
#             row["strConcluidoEduTecnico"],
#             row["strTengoEduTecnico"],
#             row["strUsuario"]
#             ))
#         count+=1
#         # con.commit()
#         print("insert row ",count," success!")
#     con.commit()





# with open('../PlataformaElectoral/EleCongreExtra2020/CandidatoEduNOUni.json','r', encoding='utf-8' ) as jsonfile:
#     doc = jsonfile.read()
#     arrayCandidatos = json.loads(str(doc))
#     count = 0
#     for row in arrayCandidatos:
#         cur.execute( \
#           "INSERT INTO public.candidato_edu_no_uni(strDocumentoIdentidad,idEstado,idHVNoUniversitaria,idHojaVida,strCarreraNoUni,strCentroEstudioNoUni,strConcluidoNoUni,strTengoNoUniversitaria,strUsuario)\
#             VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)", (
#             row["strDocumentoIdentidad"],
#             row["idEstado"],
#             row["idHVNoUniversitaria"],
#             row["idHojaVida"],
#             row["strCarreraNoUni"],
#             row["strCentroEstudioNoUni"],
#             row["strConcluidoNoUni"],
#             row["strTengoNoUniversitaria"],
#             row["strUsuario"]
#             ))
#         count+=1
#         # con.commit()
#         print("insert row ",count," success!")
#     con.commit()




# with open('../PlataformaElectoral/EleCongreExtra2020/CandidatoSentPenal.json','r', encoding='utf-8' ) as jsonfile:
#     doc = jsonfile.read()
#     arrayCandidatos = json.loads(str(doc))
#     count = 0
#     for row in arrayCandidatos:
#         cur.execute( \
#           "INSERT INTO public.candidato_sent_penal(strDocumentoIdentidad,idEstado,idHVSentenciaPenal,idHojaVida,idParamCumpleFallo,idParamModalidad,intItemSentenciaPenal,strCumpleFallo,strDelitoPenal,strExpedientePenal,strFalloPenal,strFechaSentenciaPenal,strModalidad,strOrder,strOrganoJudiPenal,strOtraModalidad,strTengoSentenciaPenal,strUsuario)\
#             VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s)", (
#             row["strDocumentoIdentidad"],
#             row["idEstado"],
#             row["idHVSentenciaPenal"],
#             row["idHojaVida"],
#             row["idParamCumpleFallo"],
#             row["idParamModalidad"],
#             row["intItemSentenciaPenal"],
#             row["strCumpleFallo"],
#             row["strDelitoPenal"],
#             row["strExpedientePenal"],
#             row["strFalloPenal"],
#             row["strFechaSentenciaPenal"],
#             row["strModalidad"],
#             row["strOrder"],
#             row["strOrganoJudiPenal"],
#             row["strOtraModalidad"],
#             row["strTengoSentenciaPenal"],
#             row["strUsuario"]
#             ))
#         count+=1
#         # con.commit()
#         print("insert row ",count," success!")
#     con.commit()





# with open('../PlataformaElectoral/EleCongreExtra2020/CandidatoSentCivil.json','r', encoding='utf-8' ) as jsonfile:
#     doc = jsonfile.read()
#     arrayCandidatos = json.loads(str(doc))
#     count = 0
#     for row in arrayCandidatos:
#         cur.execute( \
#           "INSERT INTO public.candidato_sent_civil(strDocumentoIdentidad,idEstado,idHVSentenciaObliga,idHojaVida,idParamMateriaSentencia,intItemSentenciaObliga,strEstado,strExpedienteObliga,strFalloObliga,strMateriaSentencia,strOrder,strOrganoJuridicialObliga,strTengoSentenciaObliga,strUsuario)\
#             VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s)", (
#             row["strDocumentoIdentidad"],
#             row["idEstado"],
#             row["idHVSentenciaObliga"],
#             row["idHojaVida"],
#             row["idParamMateriaSentencia"],
#             row["intItemSentenciaObliga"],
#             row["strEstado"],
#             row["strExpedienteObliga"],
#             row["strFalloObliga"],
#             row["strMateriaSentencia"],
#             row["strOrder"],
#             row["strOrganoJuridicialObliga"],
#             row["strTengoSentenciaObliga"],
#             row["strUsuario"]
#             ))
#         count+=1
#         # con.commit()
#         print("insert row ",count," success!")
#     con.commit()




# with open('../PlataformaElectoral/EleCongreExtra2020/CandidatoEduBasic.json','r', encoding='utf-8' ) as jsonfile:
#     doc = jsonfile.read()
#     arrayCandidatos = json.loads(str(doc))
#     count = 0
#     for row in arrayCandidatos:
#         cur.execute( \
#           "INSERT INTO public.candidato_edu_basic(strDocumentoIdentidad,idEstado,idHVEduBasica,idHojaVida,strConcluidoEduPrimaria,strConcluidoEduSecundaria,strEduPrimaria,strEduSecundaria,strTengoEduBasica,strUsuario)\
#             VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s,%s)", (
#             row["strDocumentoIdentidad"],
#             row["idEstado"],
#             row["idHVEduBasica"],
#             row["idHojaVida"],
#             row["strConcluidoEduPrimaria"],
#             row["strConcluidoEduSecundaria"],
#             row["strEduPrimaria"],
#             row["strEduSecundaria"],
#             row["strTengoEduBasica"],
#             row["strUsuario"]
#             ))
#         count+=1
#         # con.commit()
#         print("insert row ",count," success!")
#     con.commit()




# with open('../PlataformaElectoral/EleCongreExtra2020/CandidatoCargoPartidario.json','r', encoding='utf-8' ) as jsonfile:
#     doc = jsonfile.read()
#     arrayCandidatos = json.loads(str(doc))
#     count = 0
#     for row in arrayCandidatos:
#         cur.execute( \
#           "INSERT INTO public.candidato_cargo_partidario(strDocumentoIdentidad,idEstado,idHVCargoPartidario,idHojaVida,idOrgPolCargoPartidario,intItemCargoPartidario,strAnioCargoPartiDesde,strAnioCargoPartiHasta,strCargoPartidario,strOrder,strOrgPolCargoPartidario,strTengoCargoPartidario)\
#             VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s,%s)", (
#             row["strDocumentoIdentidad"],
#             row["idEstado"],
#             row["idHVCargoPartidario"],
#             row["idHojaVida"],
#             row["idOrgPolCargoPartidario"],
#             row["intItemCargoPartidario"],
#             row["strAnioCargoPartiDesde"],
#             row["strAnioCargoPartiHasta"],
#             row["strCargoPartidario"],
#             row["strOrder"],
#             row["strOrgPolCargoPartidario"],
#             row["strTengoCargoPartidario"]
#             ))
#         count+=1
#         # con.commit()
#         print("insert row ",count," success!")
#     con.commit()










# with open('../PlataformaElectoral/EleCongreExtra2020/CandidatoCargoEleccion.json','r', encoding='utf-8' ) as jsonfile:
#     doc = jsonfile.read()
#     arrayCandidatos = json.loads(str(doc))
#     count = 0
#     for row in arrayCandidatos:
#         cur.execute( \
#           "INSERT INTO public.candidato_cargo_eleccion(strDocumentoIdentidad,idCargoEleccion,idEstado,idHVCargoEleccion,idHojaVida,idOrgPolCargoElec,intItemCargoEleccion,strAnioCargoElecDesde,strAnioCargoElecHasta,strCargoEleccion,strCargoEleccion2,strOrder,strOrgPolCargoElec)\
#             VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s,%s,%s)", (
#             row["strDocumentoIdentidad"],
#             row["idCargoEleccion"],
#             row["idEstado"],
#             row["idHVCargoEleccion"],
#             row["idHojaVida"],
#             row["idOrgPolCargoElec"],
#             row["intItemCargoEleccion"],
#             row["strAnioCargoElecDesde"],
#             row["strAnioCargoElecHasta"],
#             row["strCargoEleccion"],
#             row["strCargoEleccion2"],
#             row["strOrder"],
#             row["strOrgPolCargoElec"]
#             ))
#         count+=1
#         # con.commit()
#         print("insert row ",count," success!")
#     con.commit()




# with open('../PlataformaElectoral/EleCongreExtra2020/Proceso.json','r', encoding='utf-8' ) as jsonfile:
#     doc = jsonfile.read()
#     arrayCandidatosData= json.loads(str(doc))
#     arrayCandidatos = arrayCandidatosData["data"]
#     count = 0
#     for row in arrayCandidatos:
#         # print(row)
#         cur.execute( \
#           "INSERT INTO public.proceso(idEstado,idProcesoElectoral,idTipoProceso,intCantidadJee,strDocConvocatoria,strEstado,strFechaAperturaProceso,strFechaConvocatoria,strFechaCierreProceso,strFechaRegistro,strNombreArchivo,strProcesoElectoral,strSiglas,strTipoProceso,strUsuario)\
#             VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (
#             row["idEstado"],
#             row["idProcesoElectoral"],
#             row["idTipoProceso"],
#             row["intCantidadJee"],
#             row["strDocConvocatoria"],
#             row["strEstado"],
#             row["strFechaAperturaProceso"],
#             row["strFechaConvocatoria"],
#             row["strFechaCierreProceso"],
#             row["strFechaRegistro"],
#             row["strNombreArchivo"],
#             row["strProcesoElectoral"],
#             row["strSiglas"],
#             row["strTipoProceso"],
#             row["strUsuario"]
#             ))
#         count+=1
#         # con.commit()
#         print("insert row ",count," success!")
#     con.commit()

con.close() 
