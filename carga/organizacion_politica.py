import psycopg2
from connexion import connect_db_target, connect_db_origin

def get_organizacion_origin():
    con = connect_db_origin()
    cur = con.cursor()
    cur.execute("select strorganizacionpolitica, 0, 1,'','', idorganizacionpolitica \
                from organizacion_politica group by idorganizacionpolitica, strorganizacionpolitica")
    data = cur.fetchall()
    con.close()
    return data
def insert_organizacion_target():
    try: 
        con = connect_db_target()
        cur = con.cursor()
        data = get_organizacion_origin()
        cur.executemany("INSERT INTO organizacion_politica(nombre, fundacion_anio, estado,\
                        descripcion, ruta_archivo, jne_idorganizacionpolitica) \
                        values(%s, %s, %s, %s, %s, %s)", data)
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