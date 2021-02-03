import psycopg2
from database import connect_db

def insert_indicador_categoria_candidato_target():
    try: 
        con = connect_db()
        cur = con.cursor()
        query = """
            INSERT INTO public.indicador_categoria_candidato(
            indicador_id,  candidato_id,indicador_categoria_id, cantidad, porcentaje, alerta, estado)
            select 1 , ca.id, ic.id, 1, 0, 1, 0
            from candidato ca
            join indicador_categoria ic on ic.order = ca.nivel_estudio_id_max and ic.indicador_id = 1
            where ca.jne_estado_lista not in ('INADMISIBLE', 'IMPROCEDENTE') and ca.jne_estado_expediente not in ('INADMISIBLE','IMPROCEDENTE','EXCLUSION', 'RETIRO', 'RENUNCIA')
            and nivel_estudio_id_max is not null ;
            -- and nivel_estudio_id_max in (1,2);
        """

        query2 = """ 
            INSERT INTO public.indicador_categoria_candidato(
            indicador_id,  candidato_id,indicador_categoria_id, cantidad, porcentaje, alerta, estado)

            select 2 ,  ca.id,  ic.id,  count(*) , 0,0,1
            from candidato_experiencia ce
            join candidato ca on ca.jne_idhojavida = ce.jne_idhojavida
            join indicador_categoria ic on ic.indicador_id = 2 and ic.nombre = ce.ocupacion_profesion
            where tipo = 3 and  ca.jne_estado_lista not in ('INADMISIBLE', 'IMPROCEDENTE') and ca.jne_estado_expediente not in ('INADMISIBLE','IMPROCEDENTE','EXCLUSION', 'RETIRO', 'RENUNCIA')
            group by 2,3;
        """
       
        query3 = """ 
             -- INDICADOR 1: categoria _nivel estudio
            INSERT INTO public.indicador_categoria_candidato(
             indicador_id,  candidato_id,indicador_categoria_id, cantidad, porcentaje, alerta, estado)
            select 8,  ca.id,  ic.id , count(*)  , 0,1,1
            from candidato_judicial cj 
            join sentencia s on s.nombre_origen = cj.sentencia
            join candidato ca on ca.jne_idhojavida = cj.jne_idhojavida
            join organizacion_politica op on op.id = ca.organizacion_politica_id
            join indicador_categoria ic on ic.nombre = s.nombre and ic.indicador_id = 8
            where cj.tipo_proceso = 'civil' and ca.jne_estado_lista not in ('INADMISIBLE', 'IMPROCEDENTE') and ca.jne_estado_expediente not in ('INADMISIBLE','IMPROCEDENTE','EXCLUSION', 'RETIRO', 'RENUNCIA')
            group by 1,2,3;

            -- Indicador 9: Sentencias penales

            INSERT INTO public.indicador_categoria_candidato(
              indicador_id,  candidato_id,indicador_categoria_id, cantidad, porcentaje, alerta, estado)
            select 9,  ca.id,  ic.id , count(*)  , 0,1,1
            from candidato_judicial cj 
            join sentencia s on s.nombre_origen = cj.sentencia
            join candidato ca on ca.jne_idhojavida = cj.jne_idhojavida
            join organizacion_politica op on op.id = ca.organizacion_politica_id
            join indicador_categoria ic on ic.nombre = s.nombre and ic.indicador_id = 9
            where cj.tipo_proceso = 'penal' and ca.jne_estado_lista not in ('INADMISIBLE', 'IMPROCEDENTE') and ca.jne_estado_expediente not in ('INADMISIBLE','IMPROCEDENTE','EXCLUSION', 'RETIRO', 'RENUNCIA')
            group by 1,2,3;

            -- Militancia en partidos anteriores

             INSERT INTO public.indicador_categoria_candidato(
              indicador_id,  candidato_id,indicador_categoria_id, cantidad, porcentaje, alerta, estado)
            select 11,  ca.id,  ( select id from indicador_categoria where indicador_id=11 and "order" = 1) , count(*)  , 0,1,1
            from candidato_experiencia ce
            join candidato ca on ca.jne_idhojavida = ce.jne_idhojavida
            left join indicador_categoria ic on ic.indicador_id = 11
            join organizacion_politica op on op.id = ca.organizacion_politica_id
            where ce.tipo =2 and similarity(ce.centro_trabajo, op.nombre) < 0.5106383 
            and similarity(ce.centro_trabajo, op.nombre) not in (0.46875,0.46153846,0.3888889)
            group by 1,2;

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
