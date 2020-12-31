import psycopg2
from connexion import connect_db

def insert_cargo_target():
    try: 
        con = connect_db()
        cur = con.cursor()
        cur.execute("INSERT INTO cargo(id, cargo, estado) \
            select idcargoeleccion, strcargoeleccion, 1  from jne.candidato_info_electoral \
            group by idcargoeleccion,strcargoeleccion")
        con.commit()
        con.close()
        print("Cargo inserts success!")
    except (Exception, psycopg2.Error) as error :
        print ("Error while fetching data", error)

    finally:
        if(con):
            cur.close()
            con.close()            

if __name__ == "__main__":
    insert_cargo_target()