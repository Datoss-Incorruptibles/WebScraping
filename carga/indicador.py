import psycopg2
from database import connect_db

def insert_indicador_target():
    try: 
        con = connect_db()
        cur = con.cursor()
        query = """
            INSERT INTO public.indicador( id, nombre, titulo, ubicacion, alerta, estado )
                select 1, 'Máximo nivel estudio', 'Estudios', 1 , 0, 1 union
                select 2, 'Trayectoria política','Trayectoria Politica', 1,0,0  union
                select 4, 'Congreso Actual','Representación Congreso Actual',1 ,0,1 union
                select 5, 'Trayectoria política reducido','Trayectoria Política',1, 0,1  union
                select 6, 'Votación vacancia Vizcarra','Votación vacancia presidencial',1 ,0, 1 union
                select 7, 'Educación Superior', 'Educ. Superior',1,0,1 union
                select 8, 'Tipos sentencias civil', 'Tipo Civil',1,0,1 union
                select 9, 'Tipos sentencias penal', 'Tipo Penal',1,0,1 union
                select 11, 'PARTIDOS ANTERIORES', 'PARTIDOS ANTERIORES',1,0,1

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
