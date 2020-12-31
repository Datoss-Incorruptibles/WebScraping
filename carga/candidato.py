import psycopg2
from connexion import connect_db

def insert_candidato_target():
    try: 
        con = connect_db()
        cur = con.cursor()
        query = """
            INSERT INTO public.candidato(jne_idcandidato, jne_idhojavida, jne_estado_lista, 
            jne_estado_expediente, jne_estado_hojavida, jne_posicion, jne_organizacion_politica,
            cargo_id, proceso_id, organizacion_politica_id, documento_identidad, apellido_paterno, 
            apellido_materno, nombres, fecha_nacimiento, profesion, region, distrito_electoral, 
            ubigeo_postula, ruta_archivo)
            SELECT cip.idcandidato, cip.idhojavida, op.strestadolista,cie.strestadoexp, 
            cip.strhojavida, cie.intposicion,opn.nombre, cie.idcargoeleccion, prc.id as procesoid, opn.id, 
            cip.strdocumentoidentidad, cip.strapellidopaterno, cip.strapellidomaterno, 
            cip.strnombres,TO_DATE(cip.strfechanacimiento,'DD/MM/YYYY'), '' profesion,
            cie.strubiregionpostula, cip.strpostuladistrito, cie.strubigeopostula, cip.strrutaarchivo
            FROM jne.candidato_info_personal cip 
            join jne.candidato_info_electoral cie on cip.strdocumentoidentidad = cie.strdocumentoidentidad 
                and cip.idcandidato = cie.idcandidato
            join jne.organizacion_politica_region op on op.idorganizacionpolitica = cie.idorganizacionpolitica 
                and op.idsolicitudlista = cie.idsolicitudlista
            join organizacion_politica opn on op.idorganizacionpolitica = jne_idorganizacionpolitica 
            left join proceso prc on prc.jne_idproceso = cie.idprocesoelectoral;
        """
        cur.execute(query)
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