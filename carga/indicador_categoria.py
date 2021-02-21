import psycopg2
from database import connect_db

def insert_indicador_categoria_target():
    try: 
        con = connect_db()
        cur = con.cursor()
        query = """
            -- INDICADOR 1: categoria _nivel estudio
            
            INSERT INTO public.indicador_categoria(indicador_id, "order", nombre, alerta, estado)
            select 1, 1, 'Primaria', 1, 1   union
            select 1, 2, 'Secundaria',1,1  union
            select 1, 3, 'Técnicos',0,1 union
            select 1, 4, 'No univ.', 0,1  union
            select 1, 5, 'Universitario',0,1 union
            select 1, 6, 'Postgrado',0,1;

            -- INDICADOR 2: categoria _trayectoria politica 

            INSERT INTO public.indicador_categoria(indicador_id, "order", nombre, alerta, estado)

            select 2, ROW_NUMBER () OVER (ORDER BY ocupacion_profesion), ocupacion_profesion, 0, 1 
            from candidato_experiencia where tipo = 3 group by ocupacion_profesion order by 2;

            -- INDICADOR 3: CANTIDAD DE CANDIDATOS           

            INSERT INTO public.indicador_categoria(indicador_id, "order", nombre, alerta, estado)
            select 3,1, 'Presidente', 0, 1 union
            select 3,4, 'Congresista', 0, 1 union
            select 3,5, 'Parlamento', 0, 1 order by 2;


            -- INDICADOR 4: categoria _congreso_actual

            INSERT INTO public.indicador_categoria(indicador_id, "order", nombre, alerta, estado)
            
            select 4,1, 'Representación en el congreso actual', 0, 1;


            -- INDICADOR 5: Trayectoria política reducido

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
            
            -- INDICADOR 6: Votación vacancia Vizcarra

            INSERT INTO indicador_categoria(indicador_id, "order", nombre, alerta, estado)
            
            select 6, 1, 'A favor',1,0 union
            select 6, 2, 'En contra',0,0 union
            select 6, 3, 'Abstención',0,0;


            -- INDICADOR 8: Tipos de sentencias civil

            INSERT INTO public.indicador_categoria(indicador_id, "order", nombre, alerta, estado)

            select 8, row_number() over (order by nombre), nombre, 1, 1 from sentencia where tipo_proceso = 'civil' group by nombre order by 1;

            -- INDICADOR 9: Tipos de sentencias  penal

            INSERT INTO public.indicador_categoria(indicador_id, "order", nombre, alerta, estado)
            select 9, row_number() over (order by nombre), nombre, 1, 1 from sentencia where tipo_proceso = 'penal' group by nombre order by 1;

            -- Indicador 10:  Ingresos promedios anuales

            INSERT INTO public.indicador_categoria(indicador_id, "order", nombre, alerta, estado)
            select 10, 1, 'Ingresos', 1, 1 union
            select 10, 2, 'Inmuebles', 1, 1 union
            select 10, 3, 'Muebles', 1, 1;

            -- INDICADOR 11:  Militantes en partidos anteriores 

            INSERT INTO public.indicador_categoria(indicador_id, "order", nombre, alerta, estado)
            select 11, 1, 'PARTIDOS ANTERIORES', 1, 1;




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
