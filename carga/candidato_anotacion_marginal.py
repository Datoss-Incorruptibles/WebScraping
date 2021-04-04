import psycopg2
from database import connect_db

def insert_candidato_anotacion():
    try: 
        con = connect_db()
        cur = con.cursor()
        query = """
            INSERT INTO public.candidato_anotacion_marginal(jne_idhojavida, jne_idanotacionmarginal, 
            jne_idtipoanotacion, jne_strtipoanotacion, dice, debedecir)
            select idhojavida, idanotacionmarginal , idtipoanotacion, strtipoanotacion, strdice, 
            strdebedecir from jne.candidato_anotacion_marginal;
        """
        cur.execute(query)
        con.commit()
        con.close()
        print("Candidato info adicional inserts success!")
    except (Exception, psycopg2.Error) as error :
        print ("Error while fetching data", error)

    finally:
        if(con):
            cur.close()
            con.close()

if __name__ == "__main__":
    insert_candidato_anotacion()