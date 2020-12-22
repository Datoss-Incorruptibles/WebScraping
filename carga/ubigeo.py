import psycopg2
from connexion import connect_db_target, connect_db_origin

def get_ubigeo_origin():
    con = connect_db_origin()
    cur = con.cursor()
    cur.execute("SELECT cie.strubigeopostula, cie.strregion, cie.strdistritoelec \
        FROM candidato_info_electoral cie GROUP BY  cie.strregion, cie.strdistritoelec, cie.strubigeopostula")
    
    lst_ubigeo = cur.fetchall()
    con.close()
    return lst_ubigeo
    
def insert_ubigeo_target():
    try: 
        con = connect_db_target()
        cur = con.cursor()
        lst_ubigeo = get_ubigeo_origin()
        cur.executemany("INSERT INTO ubigeo(ubigeo, region, distrito_electoral) \
                        values(%s, %s, %s)", lst_ubigeo)
        con.commit()
        con.close()
        print("Ubigeo inserts success!")
    except (Exception, psycopg2.Error) as error :
        print ("Error while fetching data", error)

    finally:
        if(con):
            cur.close()
            con.close()

if __name__ == "__main__":
    insert_ubigeo_target()