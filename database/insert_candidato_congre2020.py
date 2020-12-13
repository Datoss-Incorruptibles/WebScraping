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
#           "INSERT INTO public.candidato_info_electoral(idCandidato,strDocumentoIdentidad,idCargoEleccion,strCargoEleccion,strCandidato,strOrganizacionPolitica,strTipoOrganizacion,strRegion,strDistritoElec,strUbigeoPostula,idExpediente,idEstado,strEstadoExp)\
#             VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s)", (
#             row["idCandidato"],
#             row["strDocumentoIdentidad"],
#             row["idCargoEleccion"],
#             row["strCargoEleccion"],
#             row["strCandidato"],
#             row["strOrganizacionPolitica"],
#             row["strTipoOrganizacion"],
#             row["strRegion"],
#             row["strDistritoElec"],
#             row["strUbigeoPostula"],
#             row["idExpediente"],
#             row["idEstado"],
#             row["strEstadoExp"]))
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


con.close() 