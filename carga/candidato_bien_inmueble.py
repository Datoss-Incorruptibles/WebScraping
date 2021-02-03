import psycopg2
from database import connect_db

def insert_candidato_bien_inmuebles():
    try: 
        con = connect_db()
        cur = con.cursor()
        query = """
            INSERT INTO public.candidato_inmueble(jne_idhojavida, jne_idhvbieninmueble, 
            jne_strinmueblesunarp, jne_decautovaluo, direccion, valor, "order", comentario, 
            partida_sunarp, tipo)
            
            SELECT idhojavida, idhvbieninmueble, cast(strinmueblesunarp as int), decautovaluo, 
            strinmuebledireccion, case when decautovaluo = -1 then 0 else decautovaluo end as valor,
            intiteminmueble, strcomentario, strpartidasunarp, strtipobieninmueble 
            FROM jne.candidato_bien_inmueble where strtengoinmueble='1';
        """
        cur.execute(query)
        con.commit()
        con.close()
        print("Candidato bien inmuebles inserts success!")
    except (Exception, psycopg2.Error) as error :
        print ("Error while fetching data", error)

    finally:
        if(con):
            cur.close()
            con.close()

if __name__ == "__main__":
    insert_candidato_bien_inmuebles()