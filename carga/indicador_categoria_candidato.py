import psycopg2
from database import connect_db

def insert_indicador_categoria_candidato_target():
    try: 
        con = connect_db()
        cur = con.cursor()
        query = """
            INSERT INTO public.indicador_categoria_candidato(indicador_id, 
            candidato_id,indicador_categoria_id, cantidad, porcentaje, alerta, estado)
            select 1 , ca.id, ca.nivel_estudio_id_max, 1, 0, 1, 1 
            from candidato ca
            where ca.jne_estado_lista <> 'INADMISIBLE' and ca.jne_estado_expediente <> 'INADMISIBLE'
            and nivel_estudio_id_max is not null  and nivel_estudio_id_max in (1,2);

        """

        query2 = """
            select 3 ,  ca.id,  ( select id from indicador_categoria where indicador_id=3 and "order" = 1) , count(*)  , 0,1,1
            from candidato_judicial cj
            join candidato ca on ca.jne_idhojavida = cj.jne_idhojavida
            where cj.tipo_proceso = 'civil' and ca.jne_estado_lista <> 'INADMISIBLE' and ca.jne_estado_expediente <> 'INADMISIBLE'
            group by  1,2,3 ,cj.tipo_proceso

            union

            select 3 ,  ca.id,   ( select id from indicador_categoria where indicador_id=3 and "order" = 2) , count(*)  , 0,1,1
            from candidato_judicial cj
            join candidato ca on ca.jne_idhojavida = cj.jne_idhojavida
            where cj.tipo_proceso = 'penal' and  ca.jne_estado_lista <> 'INADMISIBLE' and ca.jne_estado_expediente <> 'INADMISIBLE'
            group by  1,2,3 ,cj.tipo_proceso;
        """

        query3 = """ 
            INSERT INTO public.indicador_categoria_candidato(
            indicador_id,  candidato_id,indicador_categoria_id, cantidad, porcentaje, alerta, estado)

            select 2 ,  ca.id,  ic.id,  count(*) , 0,0,1
            from candidato_experiencia ce
            join candidato ca on ca.jne_idhojavida = ce.jne_idhojavida
            join indicador_categoria ic on ic.indicador_id = 2 and ic.nombre = ce.ocupacion_profesion
            where tipo = 3 and  ca.jne_estado_lista <> 'INADMISIBLE' and ca.jne_estado_expediente <> 'INADMISIBLE'
            group by 2,3;
        """
       
        cur.execute(query)
        cur.execute(query2)
        cur.execute(query3)
        con.commit()

        
        con.close()
        print ("Indicador categoria candidato inserts success!")
    except (Exception, psycopg2.Error) as error :
        print ("Error while fetching data", error)

    finally:
        if(con):
            cur.close()
            con.close()

if __name__ == "__main__":
    insert_indicador_categoria_candidato_target()
