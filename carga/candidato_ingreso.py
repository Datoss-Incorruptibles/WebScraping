import psycopg2
from database import connect_db

def insert_candidato_ingresos():
    try: 
        con = connect_db()
        cur = con.cursor()
        query = """
            INSERT INTO public.candidato_ingreso(jne_idhojavida, jne_idhvingresos, renta_bruta_privado,
            renta_bruta_publico, renta_individual_privado, renta_individual_publico, otros_ingresos_privado, 
            otros_ingresos_publico, anio_ingresos)
            
            SELECT idhojavida, idhvingresos, decremubrutaprivado, decremubrutapublico, decrentaindividualprivado,
            decrentaindividualpublico, decotroingresoprivado, decotroingresopublico, cast(stranioingresos as int)  
            FROM jne.candidato_ingresos where strtengoingresos='1';
        """
        cur.execute(query)
        con.commit()
        con.close()
        print("Candidato ingreso inserts success!")
    except (Exception, psycopg2.Error) as error :
        print ("Error while fetching data", error)

    finally:
        if(con):
            cur.close()
            con.close()

if __name__ == "__main__":
    insert_candidato_ingresos()