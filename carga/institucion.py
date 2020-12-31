import psycopg2
from connexion import connect_db

def insert_institucion_target():
    try: 
        con = connect_db()
        cur = con.cursor()
        query = """
            INSERT INTO public.institucion(institucion)
            select strcenestudioposgrado 
            from jne.candidato_post_grado cpg
            where strtengoposgrado = '1' 
            union
            select struniversidad
            from jne.candidato_edu_uni  where strtengoeduuniversitaria = '1'
            union
            select strcentroestudionouni  
            FROM jne.candidato_edu_no_uni 
            where strtengonouniversitaria = '1' and strconcluidonouni <> '0'
            union
            select strcenestudiotecnico 
            FROM jne.candidato_edu_tecnica 
            where strtengoedutecnico = '1' and strconcluidoedutecnico <> '0';
        """
        cur.execute(query)
        con.commit()
        con.close()
        print ("Institucion inserts success!")
    except (Exception, psycopg2.Error) as error :
        print ("Error while fetching data", error)

    finally:
        if(con):
            cur.close()
            con.close()

if __name__ == "__main__":
    insert_institucion_target()