import psycopg2
from connexion import connect_db

def insert_estudio_target():
    try: 
        con = connect_db()
        cur = con.cursor()
        query = """
            INSERT INTO public.estudio(estudio)
            select strespecialidadposgrado  
            from jne.candidato_post_grado cpg
            where strtengoposgrado = '1' 
            group by strespecialidadposgrado
            union
            select strcarrerauni  
            from jne.candidato_edu_uni  
            where strtengoeduuniversitaria = '1'
            union
            select strcarreranouni  
            FROM jne.candidato_edu_no_uni 
            where strtengonouniversitaria = '1' and strconcluidonouni <> '0'
            union
            select strcarreratecnico  
            FROM jne.candidato_edu_tecnica 
            where strtengoedutecnico = '1' and strconcluidoedutecnico <> '0'
        """
        cur.execute(query)
        con.commit()
        con.close()
        print ("Estudio inserts success!")
    except (Exception, psycopg2.Error) as error :
        print ("Error while fetching data", error)

    finally:
        if(con):
            cur.close()
            con.close()

if __name__ == "__main__":
    insert_estudio_target()