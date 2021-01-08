import psycopg2
from database import connect_db

def insert_organizacion_target():
    try: 
        con = connect_db()
        cur = con.cursor()

        query = """
            INSERT INTO public.organizacion_politica(nombre, fundacion_fecha, estado, 
            descripcion, ruta_archivo, jne_idorganizacionpolitica, url) 
	        select strorganizacionpolitica, null, 1,'', 
            CONCAT('https://aplicaciones007.jne.gob.pe/srop_publico/Consulta/Simbolo/GetSimbolo/', idorganizacionpolitica),
            idorganizacionpolitica, '' from jne.organizacion_politica_region 
            group by idorganizacionpolitica, strorganizacionpolitica
        """
        cur.execute(query)
        con.commit()
        con.close()
        print ("Organizacion Politica inserts success!")
    except (Exception, psycopg2.Error) as error :
        print ("Error while fetching data", error)

    finally:
        if(con):
            cur.close()
            con.close()

if __name__ == "__main__":
    insert_organizacion_target()