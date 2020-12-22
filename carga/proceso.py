import psycopg2
from connexion import connect_db_target, connect_db_origin

def insert_proceso_target():
    try: 
        con = connect_db_target()
        cur = con.cursor()
        data = (' D.S.N.°165-2019-PCM',1,108) # change per 2021 process 
        cur.execute("INSERT INTO proceso(nombre, estado, jne_idproceso) \
                    values(%s, %s, %s)", data)
        con.commit()
        con.close()
        print("Proceso inserts success!")
    except (Exception, psycopg2.Error) as error :
        print ("Error while fetching data", error)

    finally:
        if(con):
            cur.close()
            con.close()            

if __name__ == "__main__":
    insert_proceso_target()