import psycopg2
from database import connect_db
from psycopg2.extras import execute_values

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
            cie.strubiregionpostula, cip.strpostuladistrito, cie.strubigeopostula,
            CONCAT('https://declara.jne.gob.pe', cip.strrutaarchivo) 
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


def update_candidato_target():
    try: 
        con = connect_db()
        cur = con.cursor()

        query = """
            update candidato ca  set nivel_estudio_id_max = ne.nivel_estudio_id 
            from
            (
            select ca.jne_idcandidato, ca.jne_idhojavida,
            cast(max(ce.nivel_estudio_id) as integer) nivel_estudio_id
            from candidato_estudio ce 
            left join candidato ca on  ca.jne_idhojavida = ce.jne_idhojavida
            where ce.nivel_estudio_estado <> 'NO CONCLUIDO'
            group by ca.jne_idcandidato, ca.jne_idhojavida 
            ) ne
            where ca.jne_idcandidato = ne.jne_idcandidato and ca.jne_idhojavida = ne.jne_idhojavida
        """
        query2 = """
            update candidato ca  set profesion = pr.profesion
            from
            (
            select ca.jne_idcandidato, ca.jne_idhojavida, STRING_AGG (estudio, ' ') profesion
            from candidato_estudio ce 
            left join candidato ca on  ca.jne_idhojavida = ce.jne_idhojavida
            where ca.nivel_estudio_id_max in (4) and  ce.nivel_estudio_estado in ('CONCLUIDO')  AND CE.NIVEL_ESTUDIO_ID NOT IN ('1','2')
            group by ca.jne_idcandidato, ca.jne_idhojavida 
            ) pr where ca.jne_idcandidato = pr.jne_idcandidato and ca.jne_idhojavida = pr.jne_idhojavida;

            update candidato ca  set profesion = pr.profesion
            from
            (
            select ca.jne_idcandidato, ca.jne_idhojavida, STRING_AGG (estudio, ' ') profesion
            from candidato_estudio ce 
            left join candidato ca on  ca.jne_idhojavida = ce.jne_idhojavida
            where ca.nivel_estudio_id_max in (3) and  ce.nivel_estudio_estado in ('CONCLUIDO')
            group by ca.jne_idcandidato, ca.jne_idhojavida 
            ) pr where ca.jne_idcandidato = pr.jne_idcandidato and ca.jne_idhojavida = pr.jne_idhojavida;

            update candidato ca  set profesion = pr.profesion
            from
            (
            select ca.jne_idcandidato, ca.jne_idhojavida, 'SECUNDARIA' profesion
            from candidato_estudio ce 
            left join candidato ca on  ca.jne_idhojavida = ce.jne_idhojavida
            where ca.nivel_estudio_id_max in (2) and  ce.nivel_estudio_estado in ('CONCLUIDO')
            group by ca.jne_idcandidato, ca.jne_idhojavida 
            ) pr where ca.jne_idcandidato = pr.jne_idcandidato and ca.jne_idhojavida = pr.jne_idhojavida;

            update candidato ca  set profesion = pr.profesion
            from
            (
            select ca.jne_idcandidato, ca.jne_idhojavida, 'PRIMARIA' profesion
            from candidato_estudio ce 
            left join candidato ca on  ca.jne_idhojavida = ce.jne_idhojavida
            where ca.nivel_estudio_id_max in (1) and  ce.nivel_estudio_estado in ('CONCLUIDO')
            group by ca.jne_idcandidato, ca.jne_idhojavida 
            ) pr where ca.jne_idcandidato = pr.jne_idcandidato and ca.jne_idhojavida = pr.jne_idhojavida;

            update candidato ca  set profesion = pr.profesion
            from
            (
            select ca.jne_idcandidato, ca.jne_idhojavida, STRING_AGG (estudio || ' ' || ce.nivel_estudio_estado, ',') profesion
            from candidato_estudio ce 
            left join candidato ca on  ca.jne_idhojavida = ce.jne_idhojavida
            where ca.nivel_estudio_id_max in (5) AND CE.NIVEL_ESTUDIO_ID NOT IN ('1','2', '3', '4')
                and ce.nivel_estudio_estado NOT in ('BACHILLER','UNIVERSITARIO') AND ce.nivel_estudio_estado IN ('EGRESADO','CONCLUIDO')
            group by ca.jne_idcandidato, ca.jne_idhojavida 
            ) pr where ca.jne_idcandidato = pr.jne_idcandidato and ca.jne_idhojavida = pr.jne_idhojavida;


            update candidato ca  set profesion = pr.profesion
            from
            (
            select ca.jne_idcandidato, ca.jne_idhojavida, STRING_AGG (estudio || ' ' || ce.nivel_estudio_estado, ',') profesion
            from candidato_estudio ce 
            left join candidato ca on  ca.jne_idhojavida = ce.jne_idhojavida
            where ca.nivel_estudio_id_max in (6) AND CE.NIVEL_ESTUDIO_ID NOT IN ('1','2', '3', '4') and CE.NIVEL_ESTUDIO_ID = '6'
                and ce.nivel_estudio_estado NOT in ('UNIVERSITARIO', 'MAGISTER', 'DOCTOR') AND ce.nivel_estudio_estado IN ('EGRESADO','CONCLUIDO')
            group by ca.jne_idcandidato, ca.jne_idhojavida 
            ) pr where ca.jne_idcandidato = pr.jne_idcandidato and ca.jne_idhojavida = pr.jne_idhojavida;

            update candidato ca  set profesion = pr.profesion
            from
            (
            select ca.jne_idcandidato, ca.jne_idhojavida, STRING_AGG (estudio, ',') profesion
            from candidato_estudio ce 
            left join candidato ca on  ca.jne_idhojavida = ce.jne_idhojavida
            where ca.nivel_estudio_id_max in (6) and  ce.nivel_estudio_estado in ('UNIVERSITARIO', 'MAGISTER', 'DOCTOR', 'TITULADO DOCTOR')
            group by ca.jne_idcandidato, ca.jne_idhojavida 
            ) pr where ca.jne_idcandidato = pr.jne_idcandidato and ca.jne_idhojavida = pr.jne_idhojavida;

            update candidato ca  set profesion = pr.profesion
            from
            (
            select ca.jne_idcandidato, ca.jne_idhojavida, STRING_AGG (estudio, ',') profesion
            from candidato_estudio ce 
            left join candidato ca on  ca.jne_idhojavida = ce.jne_idhojavida
            where ca.nivel_estudio_id_max in (5) and  ce.nivel_estudio_estado in ('BACHILLER','UNIVERSITARIO')
            group by ca.jne_idcandidato, ca.jne_idhojavida 
            ) pr where ca.jne_idcandidato = pr.jne_idcandidato and ca.jne_idhojavida = pr.jne_idhojavida;

            
         """
        cur.execute(query)
        cur.execute(query2)
        con.commit()

        con.close()
        print("Candidato updates success!")
    except (Exception, psycopg2.Error) as error :
        print ("Error while fetching data", error)

    finally:
        if(con):
            cur.close()
            con.close()


if __name__ == "__main__":
    #insert_candidato_target()
    update_candidato_target()