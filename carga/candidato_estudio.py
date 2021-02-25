import psycopg2
from database import connect_db

def insert_candidato_estudio_target():
    try: 
        con = connect_db()
        cur = con.cursor()
        query = """
            INSERT INTO public.candidato_estudio(jne_idhojavida, jne_idhvestudio, jne_tabla, nivel_estudio_id, 
            nivel_estudio, nivel_estudio_estado, grado, institucion, estudio, institucion_id, estudio_id, anio_bachiller, 
            anio_titulo, comentario)

            SELECT idhojavida, idhvposgrado, 'POSGRADO' jne_tabla,'6' as nivelEstudio_id, 'POSTGRADO' as nivelEstudio,  
            case    
            when cpg.strconcluidoposgrado = '2' then 'NO CONCLUIDO' 
            when cpg.strconcluidoposgrado = '1' and cpg.stregresadoposgrado = '2'  then 'CONCLUIDO' 
            when cpg.strconcluidoposgrado = '1' and cpg.stregresadoposgrado = '1' and ((cpg.stresdoctor = '' and cpg.stresmaestro = '') or (cpg.stresdoctor = '2') or (cpg.stresmaestro = '2') )   then 'EGRESADO' 
            when cpg.strconcluidoposgrado = '1' and cpg.stregresadoposgrado = '1' and (cpg.stresmaestro = '1') then 'MAGISTER' 
            when cpg.strconcluidoposgrado = '1' and cpg.stregresadoposgrado = '1' and (cpg.stresdoctor = '1' ) then 'DOCTOR' 
            else ''
            end nivel_estudio_estado,

            case    
            when cpg.strtengoposgrado = '1' and cpg.strconcluidoposgrado = '1' and cpg.stregresadoposgrado = '1' and cpg.stresdoctor = '1'  then 'DOCTOR'
            when cpg.strtengoposgrado = '1' and cpg.strconcluidoposgrado = '1' and cpg.stregresadoposgrado = '1' and cpg.stresmaestro = '1'  then 'MAGISTER'
            else ''
            end grado,
            strcenestudioposgrado as institucion , strespecialidadposgrado as estudio, i.id institucion_id, e.id estudio_id,
            '' as anioBachiller, stranioposgrado aniotitulo
            , strcomentario
            from jne.candidato_post_grado cpg
            join institucion i on i.institucion = cpg.strcenestudioposgrado
            join estudio e on e.estudio = cpg.strespecialidadposgrado
            where cpg.strtengoposgrado = '1' 

            UNION

             SELECT idhojavida, idhveduuniversitaria, 'EDU_UNI' jne_tabla, 
            CASE when strconcluidoeduuni = '1' and stregresadoeduuni = '1' and UPPER(strcarrerauni) like '%MASTER%' THEN '6'
            ELSE '5'  END nivel_estudio_id,  

            CASE when strconcluidoeduuni = '1' and stregresadoeduuni = '1' and UPPER(strcarrerauni) like '%MASTER%' THEN 'POSGRADO'
            ELSE 'UNIVERSITARIO' END,  
            case 
            when idhojavida = 134234 and idhveduuniversitaria =   138181 then 'EGRESADO'
            when idhojavida = 134259 and idhveduuniversitaria =   138513 then 'EGRESADO'
            when idhojavida = 134977 and idhveduuniversitaria = 138073 then 'UNIVERSITARIO'
            when idhojavida = 134951 and idhveduuniversitaria = 138055 then 'UNIVERSITARIO'
            when idhojavida = 137923 and idhveduuniversitaria = 142336 then 'EGRESADO'
            when idhojavida = 135963 and idhveduuniversitaria = 139523 then 'UNIVERSITARIO'

            when strconcluidoeduuni = '0' and stregresadoeduuni = '0' and UPPER(strcarrerauni) like '%BACHILLER%' and (straniobachiller <> '' or straniobachiller is not null) THEN 'CONCLUIDO' 
            when strconcluidoeduuni = '0' and stregresadoeduuni = '0' and UPPER(strcarrerauni) not like '%BACHILLER%' and (straniobachiller <> '' or straniobachiller is not null) THEN 'UNIVERSITARIO'
            when strconcluidoeduuni = '0' and stregresadoeduuni = '0' and UPPER(strcarrerauni) not like '%BACHILLER%' and (straniobachiller = '' or straniobachiller is null) THEN 'NO CONCLUIDO'
            when strconcluidoeduuni = '0' and stregresadoeduuni = '1' and UPPER(strcarrerauni) not like '%BACHILLER%' and (straniobachiller <> ''  or straniobachiller is not null) THEN 'UNIVERSITARIO'
            when strconcluidoeduuni = '0' and stregresadoeduuni = '1' and UPPER(strcarrerauni) not like '%BACHILLER%' and (straniobachiller = '' or straniobachiller is null) THEN 'EGRESADO'
            when strconcluidoeduuni = '0' and stregresadoeduuni = '2' and UPPER(strcarrerauni) not like '%BACHILLER%' and (straniobachiller = '' or straniobachiller is null) THEN 'NO CONCLUIDO'
            when strconcluidoeduuni = '1' and stregresadoeduuni = '0' and UPPER(strcarrerauni) like '%BACHILLER%' and (straniobachiller <> ''  or straniobachiller is not null) THEN 'BACHILLER'
            when strconcluidoeduuni = '1' and stregresadoeduuni = '0' and UPPER(strcarrerauni) not like '%BACHILLER%' and (straniobachiller <> ''  or straniobachiller is not null) THEN 'UNIVERSITARIO'
            when strconcluidoeduuni = '1' and stregresadoeduuni = '0' and UPPER(strcarrerauni) not like '%BACHILLER%' and (straniobachiller = '' or straniobachiller is null) THEN 'CONCLUIDO'
            when strconcluidoeduuni = '1' and stregresadoeduuni = '1' and UPPER(strcarrerauni) like '%BACHILLER%' and (straniobachiller <> ''  or straniobachiller is not null) THEN 'BACHILLER'
            when strconcluidoeduuni = '1' and stregresadoeduuni = '1' and UPPER(strcarrerauni) like '%BACHILLER%' and (straniobachiller = '' or straniobachiller is null) THEN 'BACHILLER'
            when strconcluidoeduuni = '1' and stregresadoeduuni = '1' and UPPER(strcarrerauni) like '%MASTER%' and (straniobachiller <> ''  or straniobachiller is not null) THEN 'MAGISTER'
            when strconcluidoeduuni = '1' and stregresadoeduuni = '1' and UPPER(strcarrerauni) not like '%BACHILLER%' and (straniobachiller <> ''  or straniobachiller is not null) THEN 'UNIVERSITARIO'
            when strconcluidoeduuni = '1' and stregresadoeduuni = '1' and UPPER(strcarrerauni) not like '%BACHILLER%' and (straniobachiller = '' or straniobachiller is null) THEN 'UNIVERSITARIO'
            when strconcluidoeduuni = '1' and stregresadoeduuni = '2' and UPPER(strcarrerauni) like '%BACHILLER%' and (straniobachiller <> '' or straniobachiller is not null) THEN 'BACHILLER'
            when strconcluidoeduuni = '1' and stregresadoeduuni = '2' and UPPER(strcarrerauni) not like '%BACHILLER%' and (straniobachiller <> ''  or straniobachiller is not null) THEN 'CONCLUIDO'
            when strconcluidoeduuni = '1' and stregresadoeduuni = '2' and UPPER(strcarrerauni) not like '%BACHILLER%' and (straniobachiller = '' or straniobachiller is null) THEN 'CONCLUIDO'
            when strconcluidoeduuni = '2' and stregresadoeduuni = '0' and UPPER(strcarrerauni) like '%BACHILLER%' and (straniobachiller = '' or straniobachiller is null) THEN 'NO CONCLUIDO'
            when strconcluidoeduuni = '2' and stregresadoeduuni = '0' and UPPER(strcarrerauni) not like '%BACHILLER%' and (straniobachiller <> ''  or straniobachiller is not null) THEN 'NO CONCLUIDO'
            when strconcluidoeduuni = '2' and stregresadoeduuni = '0' and UPPER(strcarrerauni) not like '%BACHILLER%' and (straniobachiller = '' or straniobachiller is null) THEN 'NO CONCLUIDO'
            when strconcluidoeduuni = '2' and stregresadoeduuni = '1' and UPPER(strcarrerauni) not like '%BACHILLER%' and (straniobachiller <> ''  or straniobachiller is not null) THEN 'EGRESADO'
            when strconcluidoeduuni = '2' and stregresadoeduuni = '1' and UPPER(strcarrerauni) not like '%BACHILLER%' and (straniobachiller = '' or straniobachiller is null) THEN 'EGRESADO'
            when strconcluidoeduuni = '2' and stregresadoeduuni = '2' and UPPER(strcarrerauni) like '%BACHILLER%' and (straniobachiller <> ''  or straniobachiller is not null) THEN 'EGRESADO'
            when strconcluidoeduuni = '2' and stregresadoeduuni = '2' and UPPER(strcarrerauni) like '%BACHILLER%' and (straniobachiller = '' or straniobachiller is null) THEN 'NO CONCLUIDO'
            when strconcluidoeduuni = '2' and stregresadoeduuni = '2' and UPPER(strcarrerauni) not like '%BACHILLER%' and (straniobachiller <> ''  or straniobachiller is not null) THEN 'NO CONCLUIDO'
            when strconcluidoeduuni = '2' and stregresadoeduuni = '2' and UPPER(strcarrerauni) not like '%BACHILLER%' and (straniobachiller = '' or straniobachiller is null) THEN 'NO CONCLUIDO'
            else 'OTROS CASOS'
            end nivel_estudio_estado,

            case 
            when strconcluidoeduuni = '1' and stregresadoeduuni = '1' and UPPER(strcarrerauni) like '%BACHILLER%' and (straniobachiller <> '' or straniobachiller is not null) THEN 'BACHILLER'
            when strconcluidoeduuni = '1' and stregresadoeduuni = '1' and UPPER(strcarrerauni) like '%BACHILLER%' and (straniobachiller = '' or straniobachiller is null) THEN 'BACHILLER'
            when strconcluidoeduuni = '1' and stregresadoeduuni = '1' and UPPER(strcarrerauni) like '%MASTER%' and (straniobachiller <> '' or straniobachiller is not null) THEN 'MAGISTER'
            when strconcluidoeduuni = '1' and stregresadoeduuni = '1' and UPPER(strcarrerauni) not like '%BACHILLER%' and (straniobachiller <> '' or straniobachiller is not null) THEN 'UNIVERSITARIO'
            when strconcluidoeduuni = '1' and stregresadoeduuni = '1' and UPPER(strcarrerauni) not like '%BACHILLER%' and (straniobachiller = '' or straniobachiller is null) THEN 'UNIVERSITARIO'

            else ''
            end grado,
            struniversidad as institucion, strcarrerauni as estudio,i.id institucion_id,e.id estudio_id, straniobachiller as aniobachiller, straniotitulo as aniotitulo
            , strcomentario
            from jne.candidato_edu_uni  
            join institucion i on i.institucion = struniversidad
            join estudio e on e.estudio = strcarrerauni
            where strtengoeduuniversitaria = '1'  and streduuniversitaria = '1' 
            AND idhojavida NOT IN (137935) AND idhveduuniversitaria NOT IN (142353) 


            UNION

                
            SELECT idhojavida, idhvnouniversitaria, 'EDU_NO_UNI' , '4' nivel_estudio_id, 'ESTUDIOS NO UNIVERSITARIOS' nivel_estudio,
            case 
            when strconcluidonouni = '1' then  'CONCLUIDO'
            when strconcluidonouni = '2' then  'NO CONCLUIDO'
            end nivel_estudio_estado,  '' grado,
            strcentroestudionouni institucion, strcarreranouni estudio, i.id institucion_id, e.id estudio_id, '' anio_bachiller, '' anio_titulo
            , '' strcomentario
            FROM jne.candidato_edu_no_uni 
            join institucion i on i.institucion = strcentroestudionouni
            join estudio e on e.estudio = strcarreranouni
            where strtengonouniversitaria = '1' and strconcluidonouni <> '0'

            UNION


            SELECT idhojavida, idhvedutecnico, 'EDU_TECNICO' , '3' nivel_estudio_id, 'ESTUDIOS TÉCNICOS' nivel_estudio,
            case 
            when strconcluidoedutecnico = '1' then  'CONCLUIDO'
            when strconcluidoedutecnico = '2' then  'NO CONCLUIDO'
            end nivel_estudio_estado,  '' grado,
            strcenestudiotecnico institucion, strcarreratecnico estudio, i.id institucion_id, e.id estudio_id, '' anio_bachiller, '' anio_titulo
            , strcomentario
            FROM jne.candidato_edu_tecnica 
            join institucion i on i.institucion = strcenestudiotecnico
            join estudio e on e.estudio = strcarreratecnico
            where strtengoedutecnico = '1' and strconcluidoedutecnico <> '0'
            UNION

            SELECT
            idhojavida, idhvedubasica, 'EDU_BASICA' tabla , 
            case 
            when streduprimaria = '1' and stredusecundaria = '1' then '2'
            when streduprimaria = '1' and stredusecundaria in ('0','2') then '1'
            end nivel_estudio_id,

            case 
            when streduprimaria = '1' and stredusecundaria = '1' then 'EDUCACIÓN SECUNDARIA'
            when streduprimaria = '1' and stredusecundaria in ('0','2') then 'EDUCACIÓN PRIMARIA'
            end nivel_estudio,


            case
            when streduprimaria = '1' and stredusecundaria = '1' and strconcluidoeduprimaria = '1' and strconcluidoedusecundaria = '1' then 'CONCLUIDO'
            when streduprimaria = '1' and stredusecundaria = '1' and strconcluidoeduprimaria = '1' and strconcluidoedusecundaria in ('0','2') then 'NO CONCLUIDO'
            when streduprimaria = '1' and stredusecundaria in ('0','2') and strconcluidoeduprimaria in ('2') and strconcluidoedusecundaria = '0' then 'NO CONCLUIDO'
            when streduprimaria = '1' and stredusecundaria in ('0','2') and strconcluidoeduprimaria in ('1') and strconcluidoedusecundaria = '0' then 'CONCLUIDO'
            else ''
            end nivel_estudio_estado,  '' grado,
            '' institucion,

            case 
            when streduprimaria = '1' and stredusecundaria = '1' then 'SECUNDARIA'
            when streduprimaria = '1' and stredusecundaria in ('0','2') then 'PRIMARIA'
            end estudio,

            0 institucion_id, 0 estudio_id, '' anio_bachiller, '' anio_titulo
            , '' strcomentario
            FROM jne.candidato_edu_basic where strtengoedubasica <> '2';
        """
        cur.execute(query)
        con.commit()
        con.close()
        print("Candidato estudio inserts success!")
    except (Exception, psycopg2.Error) as error :
        print ("Error while fetching data", error)

    finally:
        if(con):
            cur.close()
            con.close()

if __name__ == "__main__":
    insert_candidato_estudio_target()