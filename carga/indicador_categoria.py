import psycopg2
from database import connect_db

def insert_indicador_categoria_target():
    try: 
        con = connect_db()
        cur = con.cursor()
        query = """
            INSERT INTO public.indicador_categoria(indicador_id, "order", nombre, alerta, estado)
            select 1, 1, 'Educación primaria', 1, 1
            union
            select 1, 2, 'Educación secundaria',1,1
            union
            select 1, 3, 'Estudios técnicos',0,1
            union
            select 1, 4, 'Estudios no universitarios', 0,1
            union
            select 1, 5, 'Universitario',0,1
            union
            select 1, 6, 'Post grado',0,1;
        """

        query2 = """
            INSERT INTO public.indicador_categoria(indicador_id, "order", nombre, alerta, estado)
            select 2, ROW_NUMBER () OVER (ORDER BY ocupacion_profesion), ocupacion_profesion, 0, 1 
            from candidato_experiencia where tipo = 3 group by ocupacion_profesion order by 2;
        """

        query3 = """
            INSERT INTO public.indicador_categoria( indicador_id, "order", nombre, alerta, estado)
            SELECT 3, 1, 'Civil', 1,1
            union
            select 3, 2, 'Penal', 1,1;
        """

        query4 = """
            INSERT INTO public.indicador_categoria(indicador_id, "order", nombre, alerta, estado)
            SELECT 4,1, 'Congreso actual', 0, 1;
        """
        cur.execute(query)
        cur.execute(query2)
        cur.execute(query3)
        cur.execute(query4)
        con.commit()

        
        con.close()
        print ("Indicador categoria inserts success!")
    except (Exception, psycopg2.Error) as error :
        print ("Error while fetching data", error)

    finally:
        if(con):
            cur.close()
            con.close()

if __name__ == "__main__":
    insert_indicador_categoria_target()
