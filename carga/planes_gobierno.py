import psycopg2
from database import connect_db

def insert_plan_criterio():
    try: 
        con = connect_db()
        cur = con.cursor()
        query = """
            INSERT INTO public.plan_criterio(id, nombre, abreviatura, peso)	
            select 1, 'Dimensión Social', 'SOCIAL',0 UNION
            select 2, 'Dimensión Económica', 'ECONÓMICA',0 UNION
            select 3, 'Dimensión Territorial-Ambiental', 'TERRITORIAL-AMBIENTAL',0 UNION
            select 4, 'Dimensión Institucional', 'INSTITUCIONAL',0 UNION
            select 5, 'Propuestas', '',0;
        """
        cur.execute(query)
        con.commit()
        con.close()
        print ("Plan criterio inserts success!")
    except (Exception, psycopg2.Error) as error :
        print ("Error while fetching data", error)

    finally:
        if(con):
            cur.close()
            con.close()
def insert_organizacion_plan():
    try: 
        con = connect_db()
        cur = con.cursor()
        query = """
            INSERT INTO public.organizacion_plan(jne_idplangobierno, tipo_eleccion, organizacion_politica_id, ruta_archivo)
            select idplangobierno, idtipoeleccion, op.id, strrutaarchivo
            from jne.org_poli_plan_gobierno oppg
            join organizacion_politica op on op.jne_idorganizacionpolitica = oppg.idorganizacionpolitica;
        """
        cur.execute(query)
       
        con.commit()

        
        con.close()
        print ("Organizacion plan inserts success!")
    except (Exception, psycopg2.Error) as error :
        print ("Error while fetching data", error)

    finally:
        if(con):
            cur.close()
            con.close()

def insert_organizacion_plan_detalle():
    try: 
        con = connect_db()
        cur = con.cursor()
        query = """
            INSERT INTO public.organizacion_plan_detalle(plan_id, jne_idplangobierno, jne_idplangobdimension, problema, objetivo, meta, indicador, dimension_id, plan_criterio_id)
            select op.id, o.idplangobierno, o.idplangobdimension, o.strpgproblema, o.strpgobjetivo, o.strpgmeta, o.strpgindicador, o.idpgdimension, o.idpgdimension
            from jne.org_poli_plan_gobierno_dimensiones o join organizacion_plan op on op.jne_idplangobierno = o.idplangobierno;
        """
        cur.execute(query)
        con.commit()
        con.close()
        print ("Organizacion Plan Detalle inserts success!")
    except (Exception, psycopg2.Error) as error :
        print ("Error while fetching data", error)

    finally:
        if(con):
            cur.close()
            con.close()

def insert_planes():
    insert_plan_criterio()
    insert_organizacion_plan()
    insert_organizacion_plan_detalle()

if __name__ == "__main__":
    insert_planes()

