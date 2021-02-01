import psycopg2
from database import connect_db

def insert_candidato_bien_muebles():
    try: 
        con = connect_db()
        cur = con.cursor()
        query = """
            INSERT INTO public.candidato_mueble(jne_idhojavida, jne_idhvbienmueble, 
            caracteristica, comentario, marca, modelo, placa, "order", valor, vehiculo)
            
            SELECT idhojavida, idhvbienmueble, strcaracteristica, strcomentario, 
            strmarca, strmodelo, strplaca, intitemmueble, decvalor, strvehiculo 
            FROM jne.candidato_bien_mueble where strtengobienmueble = '1';
        """
        cur.execute(query)
        con.commit()
        con.close()
        print("Candidato bien muebles inserts success!")
    except (Exception, psycopg2.Error) as error :
        print ("Error while fetching data", error)

    finally:
        if(con):
            cur.close()
            con.close()

def update_candidato_bien_muebles():
    try: 
        con = connect_db()
        cur = con.cursor()
        query = """
            UPDATE public.candidato_mueble set valor = 2500.00 
            where jne_idhojavida ='135973' and jne_idhvbienmueble='146373';
        """
        cur.execute(query)
        con.commit()
        con.close()
        print("Update fix candidato bien mueble success!")
    except (Exception, psycopg2.Error) as error :
        print ("Error while fetching data", error)

    finally:
        if(con):
            cur.close()
            con.close()


if __name__ == "__main__":
#    insert_candidato_bien_muebles()
    update_candidato_bien_muebles()