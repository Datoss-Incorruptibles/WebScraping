import psycopg2
from database import connect_db

def insert_candidato_info_adicional():
    try: 
        con = connect_db()
        cur = con.cursor()
        query = """
            INSERT INTO public.candidato_info_adicional(jne_idhojavida, jne_idhvinfoadicional, info_adicional)
            select idhojavida, idhvinfoadicional, strinfoadicional from jne.candidato_info_adicional where strtengoinfoadicional='1';
        """
        cur.execute(query)
        con.commit()
        con.close()
        print("Candidato info adicional inserts success!")
    except (Exception, psycopg2.Error) as error :
        print ("Error while fetching data", error)

    finally:
        if(con):
            cur.close()
            con.close()

if __name__ == "__main__":
    insert_candidato_info_adicional()