import psycopg2
from connexion import connect_db

def insert_candidato_judicial_target():
    try: 
        con = connect_db()
        cur = con.cursor()
        query = """
            INSERT INTO public.candidato_judicial(sentencia, nro_expediente, fallo,\
            cumple_fallo, fecha_sentencia, modalidad, tipo_proceso, estado_proceso, \
            jne_idhojavida, jne_idhvsentencia)
            SELECT strdelitopenal, strexpedientepenal, strfallopenal,strcumplefallo, \
            to_date(strfechasentenciapenal,'DD/MM/YYYY'), strmodalidad, 'penal', 'sentenciado', \
            idhojavida, idhvsentenciapenal FROM jne.candidato_sent_penal \
            WHERE strtengosentenciapenal = '1' UNION
            SELECT strmateriasentencia, strexpedienteobliga, strfalloobliga,NULL, NULL, NULL,'civil',\
            'sentenciado', idhojavida, idhvsentenciaobliga FROM jne.candidato_sent_civil \
            WHERE strtengosentenciaobliga = '1';
        """
        cur.execute(query)
        con.commit()
        con.close()
        print("Candidato Judicial inserts success!")
    except (Exception, psycopg2.Error) as error :
        print ("Error while fetching data", error)

    finally:
        if(con):
            cur.close()
            con.close()

if __name__ == "__main__":
    insert_candidato_judicial_target()