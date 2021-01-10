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

            INSERT INTO public.indicador_categoria(indicador_id, "order", nombre, alerta, estado)
            select 2, ROW_NUMBER () OVER (ORDER BY ocupacion_profesion), ocupacion_profesion, 0, 1 
            from candidato_experiencia where tipo = 3 group by ocupacion_profesion order by 2;

            INSERT INTO public.indicador_categoria( indicador_id, "order", nombre, alerta, estado)
            select 3, 1, 'Sentencias Civiles', 1,1
            union
            select 3, 2, 'Sentencias Penales', 1,1;

            INSERT INTO public.indicador_categoria(indicador_id, "order", nombre, alerta, estado)
            select 4,1, 'Representación en el congreso actual', 0, 1;

            INSERT INTO indicador_categoria(indicador_id, "order", nombre, alerta, estado)

            select 5, 1, 'Presidente',0,1 union
            select 5, 2, 'Vicepresidente',0,1 union
            select 5, 3, 'Parlamento Andino',0,1 union
            select 5, 4, 'Congresista',0,1 union
            select 5, 5, 'Diputado',0,1 union
            select 5, 6, 'Gobernador',0,1 union
            select 5, 7, 'Vicegobernador',0,1 union
            select 5, 8, 'Asamblea Regional',0,1 union
            select 5, 9, 'Alcalde',0,1 union
            select 5, 10, 'Regidor',0,1 union
            select 5, 11, 'Consejero',0,1 union
            select 5, 12, 'Accesitario', 0, 1;

            INSERT INTO indicador_categoria(indicador_id, "order", nombre, alerta, estado)
            select 6, 1, 'Vacancia a Vizcarra: A favor',1,1 union
            select 6, 2, 'En contra',0,1 union
            select 6, 3, 'Abstención',0,1;

        """
        cur.execute(query)
       
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
