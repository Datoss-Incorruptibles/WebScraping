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

                   
            -- Trayectoria 

            INSERT INTO public.indicador_categoria_organizacion(
                indicador_id,  organizacion_id, indicador_categoria_id, cantidad, porcentaje, alerta, estado)
                
            select 2 indicador_id, op.id organizacion_id, ic.id,   count(*) cantidad , 0,0,1
            from candidato_experiencia ce 
            join candidato ca on ca.jne_idhojavida = ce.jne_idhojavida
            join organizacion_politica op on op.id = ca.organizacion_politica_id
            join indicador_categoria ic on ic.indicador_id = 2 and ic.nombre = ce.ocupacion_profesion
            where tipo = 3 and  ca.jne_estado_lista <> 'INADMISIBLE' and ca.jne_estado_expediente <> 'INADMISIBLE'
            group by 2,3;

            -- Congreso actual 

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

            -- trayectoria politica reducida 

            INSERT INTO public.indicador_categoria_organizacion
            (indicador_id, indicador_categoria_id, organizacion_id,  cantidad, porcentaje, alerta, estado)

            select  ic2.indicador_id, ic2.id, a.organizacion_id, a.cantidad, 0, 0,1 
            from (
            select organizacion_id, 
            case when ic.order in (10) then 1
                when ic.order in (11,18) then 2
                when ic.order in (16) then 3		
                when ic.order in (5,6) then 4 
                when ic.order in (8) then 5 
                when ic.order in (9) then 6 
                when ic.order in (19) then 7		
                when ic.order in (17) then 8	
                when ic.order in (2,3,4) then 9 
                when ic.order in (12,13,14,15) then 10		
                when ic.order in (7) then 11 	
                when ic.order in (1) then 12 	
            else 0
            end order_new
            , sum (ico.cantidad) cantidad
            from indicador_categoria_organizacion ico 
            join indicador_categoria ic on ico.indicador_categoria_id = ic.id
            where ic.indicador_id = 2 
            group by 1 ,2
            ) a join indicador_categoria ic2 on ic2.order = a.order_new 
            where ic2.indicador_id =5;

            -- Vacancia vizcarra 
        
            INSERT INTO public.indicador_categoria_organizacion
            (indicador_id, indicador_categoria_id, organizacion_id,  cantidad, porcentaje, alerta, estado)

            select 6, ic.id , op.id , 18,0,1,1 from organizacion_politica op cross join indicador_categoria ic where ic.indicador_id = 6 and op.jne_idorganizacionpolitica=4 and ic.order=1 union
            select 6, ic.id , op.id , 20,0,1,1 from organizacion_politica op cross join indicador_categoria ic where ic.indicador_id = 6 and op.jne_idorganizacionpolitica=1257 and ic.order=1 union
            select 6, ic.id , op.id , 15,0,1,1 from organizacion_politica op cross join indicador_categoria ic where ic.indicador_id = 6 and op.jne_idorganizacionpolitica=1366 and ic.order=1 union
            select 6, ic.id , op.id , 14,0,1,1 from organizacion_politica op cross join indicador_categoria ic where ic.indicador_id = 6 and op.jne_idorganizacionpolitica=2646 and ic.order=1 union
            select 6, ic.id , op.id , 12,0,1,1 from organizacion_politica op cross join indicador_categoria ic where ic.indicador_id = 6 and op.jne_idorganizacionpolitica=47 and ic.order=1 union
            select 6, ic.id , op.id , 10,0,1,1 from organizacion_politica op cross join indicador_categoria ic where ic.indicador_id = 6 and op.jne_idorganizacionpolitica=2731 and ic.order=1 union
            select 6, ic.id , op.id , 7,0,1,1 from organizacion_politica op cross join indicador_categoria ic where ic.indicador_id = 6 and op.jne_idorganizacionpolitica=14 and ic.order=1 union
            select 6, ic.id , op.id , 6,0,1,1 from organizacion_politica op cross join indicador_categoria ic where ic.indicador_id = 6 and op.jne_idorganizacionpolitica=2160 and ic.order=1 union
            select 6, ic.id , op.id , 0,0,1,1 from organizacion_politica op cross join indicador_categoria ic where ic.indicador_id = 6 and op.jne_idorganizacionpolitica=2840 and ic.order=1 union
            select 6, ic.id , op.id , 0,0,1,1 from organizacion_politica op cross join indicador_categoria ic where ic.indicador_id = 6 and op.jne_idorganizacionpolitica=4 and ic.order=2 union
            select 6, ic.id , op.id , 0,0,1,1 from organizacion_politica op cross join indicador_categoria ic where ic.indicador_id = 6 and op.jne_idorganizacionpolitica=1257 and ic.order=2 union
            select 6, ic.id , op.id , 0,0,1,1 from organizacion_politica op cross join indicador_categoria ic where ic.indicador_id = 6 and op.jne_idorganizacionpolitica=1366 and ic.order=2 union
            select 6, ic.id , op.id , 0,0,1,1 from organizacion_politica op cross join indicador_categoria ic where ic.indicador_id = 6 and op.jne_idorganizacionpolitica=2646 and ic.order=2 union
            select 6, ic.id , op.id , 0,0,1,1 from organizacion_politica op cross join indicador_categoria ic where ic.indicador_id = 6 and op.jne_idorganizacionpolitica=47 and ic.order=2 union
            select 6, ic.id , op.id , 1,0,1,1 from organizacion_politica op cross join indicador_categoria ic where ic.indicador_id = 6 and op.jne_idorganizacionpolitica=2731 and ic.order=2 union
            select 6, ic.id , op.id , 2,0,1,1 from organizacion_politica op cross join indicador_categoria ic where ic.indicador_id = 6 and op.jne_idorganizacionpolitica=14 and ic.order=2 union
            select 6, ic.id , op.id , 2,0,1,1 from organizacion_politica op cross join indicador_categoria ic where ic.indicador_id = 6 and op.jne_idorganizacionpolitica=2160 and ic.order=2 union
            select 6, ic.id , op.id , 9,0,1,1 from organizacion_politica op cross join indicador_categoria ic where ic.indicador_id = 6 and op.jne_idorganizacionpolitica=2840 and ic.order=2 union
            select 6, ic.id , op.id , 2,0,1,1 from organizacion_politica op cross join indicador_categoria ic where ic.indicador_id = 6 and op.jne_idorganizacionpolitica=4 and ic.order=3 union
            select 6, ic.id , op.id , 1,0,1,1 from organizacion_politica op cross join indicador_categoria ic where ic.indicador_id = 6 and op.jne_idorganizacionpolitica=1257 and ic.order=3 union
            select 6, ic.id , op.id , 0,0,1,1 from organizacion_politica op cross join indicador_categoria ic where ic.indicador_id = 6 and op.jne_idorganizacionpolitica=1366 and ic.order=3 union
            select 6, ic.id , op.id , 0,0,1,1 from organizacion_politica op cross join indicador_categoria ic where ic.indicador_id = 6 and op.jne_idorganizacionpolitica=2646 and ic.order=3 union
            select 6, ic.id , op.id , 0,0,1,1 from organizacion_politica op cross join indicador_categoria ic where ic.indicador_id = 6 and op.jne_idorganizacionpolitica=47 and ic.order=3 union
            select 6, ic.id , op.id , 0,0,1,1 from organizacion_politica op cross join indicador_categoria ic where ic.indicador_id = 6 and op.jne_idorganizacionpolitica=2731 and ic.order=3 union
            select 6, ic.id , op.id , 0,0,1,1 from organizacion_politica op cross join indicador_categoria ic where ic.indicador_id = 6 and op.jne_idorganizacionpolitica=14 and ic.order=3 union
            select 6, ic.id , op.id , 0,0,1,1 from organizacion_politica op cross join indicador_categoria ic where ic.indicador_id = 6 and op.jne_idorganizacionpolitica=2160 and ic.order=3 union
            select 6, ic.id , op.id , 0,0,1,1 from organizacion_politica op cross join indicador_categoria ic where ic.indicador_id = 6 and op.jne_idorganizacionpolitica=2840 and ic.order=3;


    
            -- Indicador 8: indicador_categoria_organizacion_sentencia_civil

            INSERT INTO public.indicador_categoria_organizacion(
                indicador_id,  organizacion_id, indicador_categoria_id, cantidad, porcentaje, alerta, estado)

            select 8, op.id, ic.id,  count(*), 0,1, 0
            from candidato_judicial cj 
            join sentencia s on s.nombre_origen = cj.sentencia
            join candidato ca on ca.jne_idhojavida = cj.jne_idhojavida
            join organizacion_politica op on op.id = ca.organizacion_politica_id
            join indicador_categoria ic on ic.nombre = s.nombre and ic.indicador_id = 8
            where cj.tipo_proceso = 'civil' and ca.jne_estado_lista not in ('INADMISIBLE', 'IMPROCEDENTE') and ca.jne_estado_expediente not in ('INADMISIBLE', 'IMPROCEDENTE')
            group by 1,2,3;


            -- Indicador 9: indicador_categoria_organizacion_sentencia_penal

            INSERT INTO public.indicador_categoria_organizacion(
                indicador_id,  organizacion_id, indicador_categoria_id, cantidad, porcentaje, alerta, estado)

            select 9, op.id, ic.id,  count(*), 0,1, 0
            from candidato_judicial cj 
            join sentencia s on s.nombre_origen = cj.sentencia
            join candidato ca on ca.jne_idhojavida = cj.jne_idhojavida
            join organizacion_politica op on op.id = ca.organizacion_politica_id
            join indicador_categoria ic on ic.nombre = s.nombre and ic.indicador_id = 9
            where cj.tipo_proceso = 'penal' and ca.jne_estado_lista not in ('INADMISIBLE', 'IMPROCEDENTE') and ca.jne_estado_expediente not in ('INADMISIBLE', 'IMPROCEDENTE')
            and op.id = 1
            group by 1,2,3;

            -- Indicador 10: Ingresos    

            INSERT INTO public.indicador_categoria_organizacion(
            indicador_id,  organizacion_id, indicador_categoria_id, cantidad, porcentaje, alerta, estado)

            select 10 indicador_id,  op.id organizacion_id, ic.id, count(distinct(ce.jne_idhojavida)) cantidad , 0,0,1
            from candidato_experiencia ce
            join candidato ca on ca.jne_idhojavida = ce.jne_idhojavida
            left join indicador_categoria ic on ic.indicador_id = 11
            join organizacion_politica op on op.id = ca.organizacion_politica_id
            where ce.tipo =2 and similarity(ce.centro_trabajo, op.nombre) < 0.5106383 
            and similarity(ce.centro_trabajo, op.nombre) not in (0.46875,0.46153846,0.3888889)
            group by 1,2,3  ;


            -- Indicador 11: Militantes en partidos anteriores

            INSERT INTO public.indicador_categoria_organizacion(
                indicador_id,  organizacion_id, indicador_categoria_id, cantidad, porcentaje, alerta, estado)
            select 11 indicador_id,  op.id organizacion_id, ic.id, count(distinct(ce.jne_idhojavida)) cantidad , 0,0,1
            from candidato_experiencia ce
            join candidato ca on ca.jne_idhojavida = ce.jne_idhojavida
            left join indicador_categoria ic on ic.indicador_id = 11
            join organizacion_politica op on op.id = ca.organizacion_politica_id
            where ce.tipo =2 and similarity(ce.centro_trabajo, op.nombre) < 0.5106383 
            and similarity(ce.centro_trabajo, op.nombre) not in (0.46875,0.46153846,0.3888889)
            group by 1,2,3  ;


        """ 
        cur.execute(query)
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
