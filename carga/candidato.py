import psycopg2
from connexion import connect_db_target, connect_db_origin

def get_candidato_origin():
    con = connect_db_origin()
    cur = con.cursor()
    query= "select cip.idcandidato, cip.idhojavida,  op.strestadolista,cie.strestadoexp,\
            cip.strhojavida, cie.intposicion,cie.strorganizacionpolitica, \
            4 cargo_id, prc.id proceso, opn.id organizacion_politica_id, \
            cip.strdocumentoidentidad, cip.strapellidopaterno, cip.strapellidomaterno, \
            cip.strnombres,TO_DATE(cip.strfechanacimiento,'DD/MM/YYYY'), '' profesion, \
            cie.strregion, cie.strdistritoelec, cie.strubigeopostula, cip.strrutaarchivo \
            from candidato_info_personal cip join candidato_info_electoral cie on \
            cip.strdocumentoidentidad = cie.strdocumentoidentidad   join organizacion_politica op \
            on op.idexpediente = cie.idexpediente join _organizacion_politica opn on \
            op.idorganizacionpolitica = jne_idorganizacionpolitica join _proceso prc on \
            prc.jne_idproceso = cie.idprocesoelectoral"
    cur.execute(query)
    data = cur.fetchall() 
#    for candidato in data:
#        print(candidato)
    con.close()
    return data

def insert_candidato_target():
    try: 
        con = connect_db_target()
        cur = con.cursor()
        data = get_candidato_origin()
        cur.executemany("INSERT INTO candidato(jne_idcandidato, jne_idhojavida, \
                        jne_estado_lista, jne_estado_expediente, jne_estado_hojavida, jne_posicion, \
                        jne_organizacion_politica,cargo_id, proceso_id, organizacion_politica_id, \
                        documento_identidad, apellido_paterno, apellido_materno, nombres, fecha_nacimiento, \
                        profesion, region, distrito_electoral, ubigeo_postula, ruta_archivo) \
                        values(%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s)", data)
        con.commit()
        con.close()
        print("Candidato inserts success!")
    except (Exception, psycopg2.Error) as error :
        print ("Error while fetching data", error)

    finally:
        if(con):
            cur.close()
            con.close()

if __name__ == "__main__":
    insert_candidato_target()