import psycopg2
from connexion import connect_db

def insert_proceso_target():
    try: 
        con = connect_db()
        cur = con.cursor()
        data = ('ELECCIONES GENERALES 2021',1,110) # change per 2021 process 
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