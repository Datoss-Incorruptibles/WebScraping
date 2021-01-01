import psycopg2
from database import connect_db

def insert_organizacion_target():
    try: 
        con = connect_db()
        cur = con.cursor()
        cur.execute("""INSERT INTO public.organizacion_politica(nombre, fundacion_anio, estado, \
            descripcion, ruta_archivo, jne_idorganizacionpolitica) \
	        select strorganizacionpolitica, 0, 1,'','', idorganizacionpolitica from jne.organizacion_politica_region \
            group by idorganizacionpolitica, strorganizacionpolitica""")
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