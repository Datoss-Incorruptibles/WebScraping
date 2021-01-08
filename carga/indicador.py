import psycopg2
from database import connect_db

def insert_indicador_target():
    try: 
        con = connect_db()
        cur = con.cursor()
        query = """
            INSERT INTO public.indicador( id, nombre, alerta, estado )
            select 1, 'Máximo nivel estudio', 0, 1
            union
            select 2, 'Trayectoria politica',0,1
            union
            select 3, 'Procesos Judiciales',1,1
            union
            select 4, 'Congreso Actual',0,1
        """
        cur.execute(query)
        con.commit()
        con.close()
        print ("Indicador inserts success!")
    except (Exception, psycopg2.Error) as error :
        print ("Error while fetching data", error)

    finally:
        if(con):
            cur.close()
            con.close()

if __name__ == "__main__":
    insert_indicador_target()
