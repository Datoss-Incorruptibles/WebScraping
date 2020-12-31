import psycopg2
from connexion import connect_db

    
def insert_ubigeo_target():
    try: 
        con = connect_db()
        cur = con.cursor()
        cur.execute("""
            INSERT INTO ubigeo(ubigeo, region, distrito_electoral)
            select cie.strubigeopostula, cie.strubiregionpostula,  cip.strpostuladistrito 
            from jne.candidato_info_electoral cie 
            join jne.candidato_info_personal cip on cip.idcandidato = cie.idcandidato and cip.idhojavida = cie.idhojavida
            where cie.strubigeopostula <> ''
            group by cie.strubigeopostula, cie.strubiregionpostula,  cip.strpostuladistrito;""")
        con.commit()
        con.close()
        print("Ubigeo inserts success!")
    except (Exception, psycopg2.Error) as error :
        print ("Error while fetching data", error)

    finally:
        if(con):
            cur.close()
            con.close()

if __name__ == "__main__":
    insert_ubigeo_target()