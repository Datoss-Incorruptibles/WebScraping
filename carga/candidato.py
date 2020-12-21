from connexion import connect_db_target, connect_db_origin
import csv

cono = connect_db_origin()
cond = connect_db_target()

curo = cono.cursor()
query= "select cip.idcandidato, cip.idhojavida, cip.idcargoeleccion, cip.strapellidopaterno, cip.strapellidomaterno,\
        cip.strnombres,cip.strfechanacimiento, cip.strdocumentoidentidad, cip.strestado,  cip.strrutaarchivo, \
        cie.strorganizacionpolitica, cie.strregion, cie.strdistritoelec, cie.strubigeopostula,cie.idestado, \
        cie.strestadoexp from candidato_info_personal cip \
        inner join candidato_info_electoral cie on cip.strdocumentoidentidad = cie.strdocumentoidentidad"
curo.execute(query)

lst_candidato = curo.fetchall() 
for candidato in lst_candidato:
    print(candidato)
