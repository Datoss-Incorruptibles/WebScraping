import psycopg2
from database import connect_db

def insert_indicador_categoria_organizacion_target():
    try: 
        con = connect_db()
        cur = con.cursor()
        query = """
            INSERT INTO public.indicador_categoria_organizacion(indicador_id,  organizacion_id, 
            indicador_categoria_id, cantidad, porcentaje, alerta, estado)
            select 1, op.id , ic.id, count(distinct(documento_identidad)) , 0, 0, 1
            from candidato ca
            join organizacion_politica op on op.id = ca.organizacion_politica_id
            join indicador_categoria ic on ic.indicador_id = 1 and ca.nivel_estudio_id_max = ic.order
            where ca.jne_estado_lista <> 'INADMISIBLE' and ca.jne_estado_expediente <> 'INADMISIBLE' 
            and nivel_estudio_id_max is not null
            group by op.id , ic.id
            order by 1;
        """

        query2 = """
            INSERT INTO public.indicador_categoria_organizacion(
            indicador_id,  organizacion_id, indicador_categoria_id, cantidad, porcentaje, alerta, estado)
            
            select 3 indicador_id,  op.id organizacion_id, ( select id from indicador_categoria where indicador_id=3 and "order" = 1) indicador_categoria_id, count(*) cantidad , 0,0,1
            from candidato_judicial cj 
            join candidato ca on ca.jne_idhojavida = cj.jne_idhojavida
            join organizacion_politica op on op.id = ca.organizacion_politica_id
            where cj.tipo_proceso = 'civil' and ca.jne_estado_lista <> 'INADMISIBLE' and ca.jne_estado_expediente <> 'INADMISIBLE'
            group by 3, op.id, indicador_categoria_id

            union 

            select 3 indicador_id,  op.id organizacion_id,  ( select id from indicador_categoria where indicador_id=3 and "order" = 2)  indicador_categoria_id, count(*) cantidad , 0,0,1
            from candidato_judicial cj 
            join candidato ca on ca.jne_idhojavida = cj.jne_idhojavida
            join organizacion_politica op on op.id = ca.organizacion_politica_id
            where cj.tipo_proceso = 'penal' and  ca.jne_estado_lista <> 'INADMISIBLE' and ca.jne_estado_expediente <> 'INADMISIBLE'
            group by 3, op.id, indicador_categoria_id;   

        """

        query3 = """
            
            INSERT INTO public.indicador_categoria_organizacion(
                indicador_id,  organizacion_id, indicador_categoria_id, cantidad, porcentaje, alerta, estado)
                
            select 2 indicador_id, op.id organizacion_id, ic.id,   count(*) cantidad , 0,0,1
            from candidato_experiencia ce 
            join candidato ca on ca.jne_idhojavida = ce.jne_idhojavida
            join organizacion_politica op on op.id = ca.organizacion_politica_id
            join indicador_categoria ic on ic.indicador_id = 2 and ic.nombre = ce.ocupacion_profesion
            where tipo = 3 and  ca.jne_estado_lista <> 'INADMISIBLE' and ca.jne_estado_expediente <> 'INADMISIBLE'
            group by 2,3;

        """

        query4 = """
            INSERT INTO public.indicador_categoria_organizacion
            (indicador_id, indicador_categoria_id, organizacion_id,  cantidad, porcentaje, alerta, estado)

            select 4, ( select id from indicador_categoria where indicador_id=4 and "order" = 1) 
            ,op.id,25,10.26, 0,1 from organizacion_politica op where op.jne_idorganizacionpolitica = 4 union
            select 4, ( select id from indicador_categoria where indicador_id=4 and "order" = 1) 
            ,op.id,22,7.96,0,1 from organizacion_politica op where op.jne_idorganizacionpolitica = 1257 union
            select 4, ( select id from indicador_categoria where indicador_id=4 and "order" = 1) 
            ,op.id,15,8.38,0,1 from organizacion_politica op where op.jne_idorganizacionpolitica = 2646 union
            select 4, ( select id from indicador_categoria where indicador_id=4 and "order" = 1) 
            ,op.id,15,7.31, 0,1 from organizacion_politica op where op.jne_idorganizacionpolitica = 1366 union
            select 4, ( select id from indicador_categoria where indicador_id=4 and "order" = 1) 
            ,op.id,13, 6.77 , 0,1 from organizacion_politica op where op.jne_idorganizacionpolitica = 47 union
            select 4, ( select id from indicador_categoria where indicador_id=4 and "order" = 1) 
            ,op.id,11,6.05,0,1 from organizacion_politica op where op.jne_idorganizacionpolitica = 14 union
            select 4, ( select id from indicador_categoria where indicador_id=4 and "order" = 1) 
            ,op.id,11,8.38,0,1 from organizacion_politica op where op.jne_idorganizacionpolitica = 2731 union
            select 4, ( select id from indicador_categoria where indicador_id=4 and "order" = 1) 
            ,op.id,9,6.16,0,1 from organizacion_politica op where op.jne_idorganizacionpolitica = 2160 union
            select 4, ( select id from indicador_categoria where indicador_id=4 and "order" = 1) 
            ,op.id,9,7.40,0,1 from organizacion_politica op where op.jne_idorganizacionpolitica = 2840;

        """
        cur.execute(query)
        cur.execute(query2)
        cur.execute(query3)
        cur.execute(query4)
        con.commit()

        
        con.close()
        print ("Indicador categoria organizacion inserts success!")
    except (Exception, psycopg2.Error) as error :
        print ("Error while fetching data", error)

    finally:
        if(con):
            cur.close()
            con.close()

if __name__ == "__main__":
    insert_indicador_categoria_organizacion_target()
