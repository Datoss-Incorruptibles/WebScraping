import psycopg2
from database import connect_db

def insert_candidato_experiencia_target():
    try: 
        con = connect_db()
        cur = con.cursor()
        query = """
            INSERT INTO public.candidato_experiencia(tipo, jne_idhojavida, jne_idhvexpelaboral, 
            item_expelaboral, centro_trabajo, ocupacion_profesion, anio_trabajo_desde, anio_trabajo_hasta)

            select 1 tipo , idhojavida, idhvexpelaboral, intitemexpelaboral,strcentrotrabajo, 
            strocupacionprofesion, straniotrabajodesde, straniotrabajohasta from jne.candidato_exp_laboral 
            where strtengoexpelaboral='1'
            union
            select 2 tipo, idhojavida, idhvcargopartidario, intitemcargopartidario, strorgpolcargopartidario,
            strcargopartidario, straniocargopartidesde, straniocargopartihasta from jne.candidato_cargo_partidario 
            where strtengocargopartidario = '1'
            union
            select 3 tipo, idhojavida, idhvcargoeleccion, intitemcargoeleccion,  strorgpolcargoelec, strcargoeleccion2, 
            straniocargoelecdesde, straniocargoelechasta from jne.candidato_cargo_eleccion where strcargoeleccion = '1';
        """
        cur.execute(query)
        con.commit()
        con.close()
        print("Candidato Experiencia inserts success!")
    except (Exception, psycopg2.Error) as error :
        print ("Error while fetching data", error)

    finally:
        if(con):
            cur.close()
            con.close()

if __name__ == "__main__":
    insert_candidato_experiencia_target()