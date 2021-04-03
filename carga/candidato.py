import psycopg2
from database import connect_db
from psycopg2.extras import execute_values

def insert_candidato_target():
    try: 
        con = connect_db()
        cur = con.cursor()
        query = """
            INSERT INTO public.candidato(jne_idcandidato, jne_idhojavida, jne_estado_lista, 
            jne_estado_expediente, jne_estado_hojavida, jne_posicion, jne_organizacion_politica,
            cargo_id, proceso_id, organizacion_politica_id, documento_identidad, apellido_paterno, 
            apellido_materno, nombres, fecha_nacimiento, profesion, region, distrito_electoral, 
            ubigeo_postula, ruta_archivo)
            SELECT cie.idcandidato, cie.idhojavida, op.strestadolista,cie.strestadoexp, 
            cip.strhojavida, cie.intposicion,opn.nombre, cie.idcargoeleccion, prc.id as procesoid, opn.id, 
            cip.strdocumentoidentidad, cip.strapellidopaterno, cip.strapellidomaterno, 
            cip.strnombres,TO_DATE(cip.strfechanacimiento,'DD/MM/YYYY'), '' profesion,
            cie.strubiregionpostula, cip.strpostuladistrito, cie.strubigeopostula,
            CONCAT('https://declara.jne.gob.pe', cip.strrutaarchivo) 
            FROM jne.candidato_info_electoral cie           
            join jne.candidato_info_personal cip on cie.idhojavida = cip.idhojavida
            join jne.organizacion_politica_region op on op.idorganizacionpolitica = cie.idorganizacionpolitica 
             and op.idsolicitudlista = cie.idsolicitudlista
             left join organizacion_politica opn on op.idorganizacionpolitica = jne_idorganizacionpolitica 
             left join proceso prc on prc.jne_idproceso = cie.idprocesoelectoral;
             --where op.strestadolista not in ('INADMISIBLE', 'IMPROCEDENTE')  -- no aplicar 
             --and cie.strestadoexp not in ('INADMISIBLE', 'EXCLUSION', 'RETIRO', 'IMPROCEDENTE','RENUNCIA');       
        """
        cur.execute(query)

        query_fix = """
                delete from candidato where id = (select id from candidato where jne_idhojavida = 136564 limit 1);
        """

        cur.execute(query_fix)
        con.commit()

        con.close()
        print("Candidato inserts success!")
    except (Exception, psycopg2.Error) as error :
        print ("Error while fetching data", error)

    finally:
        if(con):
            cur.close()
            con.close()


def update_candidato_target():
    try: 
        con = connect_db()
        cur = con.cursor()

        query = """
            update candidato ca  set nivel_estudio_id_max = ne.nivel_estudio_id 
            from
            (
            select ca.jne_idcandidato, ca.jne_idhojavida,
            cast(max(ce.nivel_estudio_id) as integer) nivel_estudio_id
            from candidato_estudio ce 
            left join candidato ca on  ca.jne_idhojavida = ce.jne_idhojavida
            where ce.nivel_estudio_estado <> 'NO CONCLUIDO'
            group by ca.jne_idcandidato, ca.jne_idhojavida 
            ) ne
            where ca.jne_idcandidato = ne.jne_idcandidato and ca.jne_idhojavida = ne.jne_idhojavida;

            update candidato ca  set nivel_estudio_id_max = ne.nivel_estudio_id 
            from
            (
            SELECT ca.jne_idcandidato, ca.jne_idhojavida, 1 nivel_estudio_id
            from candidato ca 
            where  ca.jne_estado_lista not in ('INADMISIBLE', 'IMPROCEDENTE') and ca.jne_estado_expediente not in ('INADMISIBLE','IMPROCEDENTE','EXCLUSION', 'RETIRO', 'RENUNCIA')
            and nivel_estudio_id_max is null  
            ) ne
            where ca.jne_idcandidato = ne.jne_idcandidato and ca.jne_idhojavida = ne.jne_idhojavida;

        """
        query2 = """
            update candidato ca  set profesion = pr.profesion
            from
            (
            select ca.jne_idcandidato, ca.jne_idhojavida, STRING_AGG (estudio, ', ') profesion
            from candidato_estudio ce 
            left join candidato ca on  ca.jne_idhojavida = ce.jne_idhojavida
            where ca.nivel_estudio_id_max in (4) and  ce.nivel_estudio_estado in ('CONCLUIDO')  AND CE.NIVEL_ESTUDIO_ID NOT IN ('1','2')
            group by ca.jne_idcandidato, ca.jne_idhojavida 
            ) pr where ca.jne_idcandidato = pr.jne_idcandidato and ca.jne_idhojavida = pr.jne_idhojavida;

            update candidato ca  set profesion = pr.profesion
            from
            (
            select ca.jne_idcandidato, ca.jne_idhojavida, STRING_AGG (estudio, ', ') profesion
            from candidato_estudio ce 
            left join candidato ca on  ca.jne_idhojavida = ce.jne_idhojavida
            where ca.nivel_estudio_id_max in (3) and  ce.nivel_estudio_estado in ('CONCLUIDO') AND CE.NIVEL_ESTUDIO_ID NOT IN ('1','2')
            group by ca.jne_idcandidato, ca.jne_idhojavida 
            ) pr where ca.jne_idcandidato = pr.jne_idcandidato and ca.jne_idhojavida = pr.jne_idhojavida;

            update candidato ca  set profesion = pr.profesion
            from
            (
            select ca.jne_idcandidato, ca.jne_idhojavida, 'SECUNDARIA' profesion
            from candidato_estudio ce 
            left join candidato ca on  ca.jne_idhojavida = ce.jne_idhojavida
            where ca.nivel_estudio_id_max in (2) and  ce.nivel_estudio_estado in ('CONCLUIDO')
            group by ca.jne_idcandidato, ca.jne_idhojavida 
            ) pr where ca.jne_idcandidato = pr.jne_idcandidato and ca.jne_idhojavida = pr.jne_idhojavida;

            update candidato ca  set profesion = pr.profesion
            from
            (
            select ca.jne_idcandidato, ca.jne_idhojavida, 'PRIMARIA' profesion
            from candidato_estudio ce 
            left join candidato ca on  ca.jne_idhojavida = ce.jne_idhojavida
            where ca.nivel_estudio_id_max in (1) and  ce.nivel_estudio_estado in ('CONCLUIDO')
            group by ca.jne_idcandidato, ca.jne_idhojavida 
            ) pr where ca.jne_idcandidato = pr.jne_idcandidato and ca.jne_idhojavida = pr.jne_idhojavida;

            update candidato ca  set profesion = pr.profesion
            from
            (
            select ca.jne_idcandidato, ca.jne_idhojavida, STRING_AGG (estudio || ' ' || ce.nivel_estudio_estado, ',') profesion
            from candidato_estudio ce 
            left join candidato ca on  ca.jne_idhojavida = ce.jne_idhojavida
            where ca.nivel_estudio_id_max in (5) AND CE.NIVEL_ESTUDIO_ID NOT IN ('1','2', '3', '4')
                and ce.nivel_estudio_estado NOT in ('BACHILLER','UNIVERSITARIO') AND ce.nivel_estudio_estado IN ('EGRESADO','CONCLUIDO')
            group by ca.jne_idcandidato, ca.jne_idhojavida 
            ) pr where ca.jne_idcandidato = pr.jne_idcandidato and ca.jne_idhojavida = pr.jne_idhojavida;


            update candidato ca  set profesion = pr.profesion
            from
            (
            select ca.jne_idcandidato, ca.jne_idhojavida, STRING_AGG (estudio || ' ' || ce.nivel_estudio_estado, ',') profesion
            from candidato_estudio ce 
            left join candidato ca on  ca.jne_idhojavida = ce.jne_idhojavida
            where ca.nivel_estudio_id_max in (6) AND CE.NIVEL_ESTUDIO_ID NOT IN ('1','2', '3', '4') and CE.NIVEL_ESTUDIO_ID = '6'
                and ce.nivel_estudio_estado NOT in ('UNIVERSITARIO', 'MAGISTER', 'DOCTOR') AND ce.nivel_estudio_estado IN ('EGRESADO','CONCLUIDO')
            group by ca.jne_idcandidato, ca.jne_idhojavida 
            ) pr where ca.jne_idcandidato = pr.jne_idcandidato and ca.jne_idhojavida = pr.jne_idhojavida;

            update candidato ca  set profesion = pr.profesion
            from
            (
            select ca.jne_idcandidato, ca.jne_idhojavida, STRING_AGG (estudio, ', ') profesion
            from candidato_estudio ce 
            left join candidato ca on  ca.jne_idhojavida = ce.jne_idhojavida
            where ca.nivel_estudio_id_max in (6) and  ce.nivel_estudio_estado in ('UNIVERSITARIO', 'MAGISTER', 'DOCTOR', 'TITULADO DOCTOR')
            group by ca.jne_idcandidato, ca.jne_idhojavida 
            ) pr where ca.jne_idcandidato = pr.jne_idcandidato and ca.jne_idhojavida = pr.jne_idhojavida;

            update candidato ca  set profesion = pr.profesion
            from
            (
            select ca.jne_idcandidato, ca.jne_idhojavida, STRING_AGG (estudio, ', ') profesion
            from candidato_estudio ce 
            left join candidato ca on  ca.jne_idhojavida = ce.jne_idhojavida
            where ca.nivel_estudio_id_max in (5) and  ce.nivel_estudio_estado in ('BACHILLER','UNIVERSITARIO')
            group by ca.jne_idcandidato, ca.jne_idhojavida 
            ) pr where ca.jne_idcandidato = pr.jne_idcandidato and ca.jne_idhojavida = pr.jne_idhojavida;

            update candidato set profesion = 'ABOGADO' where jne_idhojavida = 134133;
            update candidato set profesion = 'INGENIERO QUIMICO' where jne_idhojavida = 136626;
            update candidato set profesion = 'ARQUITECTO' where jne_idhojavida = 134382;
            update candidato set profesion = 'BACHILLER EN CIENCIAS MILITARES' where jne_idhojavida = 137291;
            update candidato set profesion = 'ABOGADO' where jne_idhojavida = 134127;
            update candidato set profesion = 'INGENIERO INDUSTRIAL, DOCTOR EN ADMINISTRACION ESTRATEGICA DE EMPRESAS' where jne_idhojavida = 134982;
            update candidato set profesion = 'ABOGADO' where jne_idhojavida = 133886;
            update candidato set profesion = 'INGENIERO INDUSTRIAL, DOCTOR EN ADMINISTRACION ESTRATEGICA DE EMPRESAS' where jne_idhojavida = 134982;
            update candidato set profesion = 'INGENIERO CIVIL' where jne_idhojavida = 136570;
            update candidato set profesion = 'MEDICO CIRUJANO' where jne_idhojavida = 134453;
            update candidato set profesion = 'TÍTULO PROFESIONAL TÉCNICO EN TÉCNICA EN FARMACIA' where jne_idhojavida =134969;
            update candidato set profesion = 'ADMINISTRACION DE EMPRESAS, MAESTRÍA  EN ADMINISTRACIÓN DE EMPRESAS' where jne_idhojavida = 133878;
            update candidato set profesion = 'ABOGADO, MAGÍSTER EN DERECHO PENAL' where jne_idhojavida = 134976;
            update candidato set profesion = 'ABOGADO, GRADO DE MAESTRO EN DERECHO (GRADO DE MAESTRO)' where jne_idhojavida = 135035;
            update candidato set profesion = 'LICENCIADO EN ECONOMÍA, MAESTRÍA EN ADMINISTRACIÓN DE NEGOCIOS (MBA)' where jne_idhojavida = 135744;
            update candidato set profesion = 'ABOGADA, DOCTORA EN GESTION PUBLICA Y GOBERNABILIDAD' where jne_idhojavida = 135407;
            update candidato set profesion = 'ABOGADO UNIVERSIDAD PRIVADA DE TACNA, MAGISTER EN DERECHO PROCESAL PENAL' where jne_idhojavida = 137817;
            update candidato set profesion = 'BACHILLER EN CIENCIAS-INGENIERÍA AGRÍCOLA' where jne_idhojavida = 137553;
            update candidato set profesion = 'BACHILLER EN SOCIOLOGÍA' where jne_idhojavida = 137384;
            update candidato set profesion = 'CONTADOR PÚBLICO, MAGÍSTER EN DIRECCIÓN DE EMPRESAS' where jne_idhojavida = 134123;
            update candidato set profesion = 'INGENIERO ECONOMISTA' where jne_idhojavida = 137294;
            update candidato set profesion = 'SECUNDARIA' where jne_idhojavida = 135714;
            update candidato set profesion = 'LICENCIADO EN CIENCIAS DE LA COMUNICACIÓN, ABOGADO' where jne_idhojavida = 135443;
            update candidato set profesion = 'BACHILLER EN INGENIERIA DE MINAS' where jne_idhojavida = 135866;
            update candidato set profesion = 'LICENCIADA EN ADMINISTRACIÓN' where jne_idhojavida = 134290;
            update candidato set profesion = 'LICENCIADO EN ADMINISTRACION Y CIENCIAS POLICIALES, ABOGADO, BACHILLER EN EDUCACIÓN' where jne_idhojavida = 133847;
            update candidato set profesion = 'LICENCIADO EN ADMINISTRACION' where jne_idhojavida = 133873;
            update candidato set profesion = 'ABOGADO' where jne_idhojavida = 134032;
            update candidato set profesion = 'INGENIERO INDUSTRIAL' where jne_idhojavida = 135417;
            update candidato set profesion = 'ABOGADO' where jne_idhojavida = 133855;
            update candidato set profesion = 'CIRUJANO DENTISTA,BACHILLER EN CIENCIAS ADMINISTRATIVAS' where jne_idhojavida = 134371;
            update candidato set profesion = 'ABOGADO' where jne_idhojavida = 134075;
            update candidato set profesion = 'LICENCIADO EN PERIODISMO' where jne_idhojavida = 133993;
            update candidato set profesion = 'LICENCIADO EN CIENCIAS DE LA COMUNICACION' where jne_idhojavida = 133916;
            update candidato set profesion = 'ABOGADO' where jne_idhojavida = 134345;
            update candidato set profesion = 'ENFERMERA' where jne_idhojavida = 134201;
            update candidato set profesion = 'SECUNDARIA' where jne_idhojavida = 133839;
            update candidato set profesion = 'ECONOMISTA' where jne_idhojavida = 134375;
            update candidato set profesion = 'LICENCIADO EN CIENCIAS MILITARES' where jne_idhojavida = 135743;
            update candidato set profesion = 'ABOGADO' where jne_idhojavida =133802;
            update candidato set profesion = 'ABOGADO' where jne_idhojavida = 133812;
            update candidato set profesion = 'LICENCIADA EN TURISMO' where jne_idhojavida = 134446;
            update candidato set profesion = 'ABOGADO' where jne_idhojavida = 134148;
            update candidato set profesion = 'BACHILLER EN CIENCIAS AGRARIAS' where jne_idhojavida = 135596;
            update candidato set profesion = 'ABOGADA' where jne_idhojavida = 135824;
            update candidato set profesion = 'CONTADOR PUBLICO' where jne_idhojavida = 135601;
            update candidato set profesion = 'LICENCIADO EN EDUCACION SECUNDARIA' where jne_idhojavida = 134193;
            update candidato set profesion = 'ABOGADO' where jne_idhojavida = 134683;
            update candidato set profesion = 'ABOGADO' where jne_idhojavida = 134277;
            update candidato set profesion = 'LICENCIADO EN ADMINISTRACION' where jne_idhojavida = 135713;
            update candidato set profesion = 'CIRUJANO DENTISTA' where jne_idhojavida = 134636;
            update candidato set profesion = 'ARQUITECTA' where jne_idhojavida = 134643;
            update candidato set profesion = 'LICENCIADO EN PERIODISMO' where jne_idhojavida = 133942;
            update candidato set profesion = 'ABOGADO' where jne_idhojavida = 133879;
            update candidato set profesion = 'INGENIERO ADMINISTRATIVO' where jne_idhojavida = 135185;
            update candidato set profesion = 'ABOGADO' where jne_idhojavida = 134181;
            update candidato set profesion = 'ABOGADO' where jne_idhojavida = 136417;
            update candidato set profesion = 'BACHILLER EN INGENIERÍA DE MINAS' where jne_idhojavida = 136372;
            update candidato set profesion = 'ABOGADO' where jne_idhojavida = 134156;
            update candidato set profesion = 'LICENCIADO EN EDUCACION, ESPECIALIDAD: AREA PRINCIPAL: LENGUA ESPAÑOLA AREA SECUNDARIA: LITERATURA' where jne_idhojavida = 134325;
            update candidato set profesion = 'LICENCIADO EN CIENCIAS DE LA COMUNICACIÓN, BACHILLER EN INGENIERIA DE SISTEMAS' where jne_idhojavida = 135696;
            update candidato set profesion = 'INGENIERO MECÁNICO' where jne_idhojavida = 134067;
            update candidato set profesion = 'LICENCIADA EN EDUCACIÓN PRIMARIA' where jne_idhojavida = 134078;
            update candidato set profesion = 'INGENIERO QUIMICO' where jne_idhojavida = 134320;
            update candidato set profesion = 'ABOGADO' where jne_idhojavida = 134197;
            update candidato set profesion = 'TRABAJADOR SOCIAL' where jne_idhojavida = 133836;
            update candidato set profesion = 'ABOGADO' where jne_idhojavida = 133752;
            update candidato set profesion = 'LICENCIADO EN PERIODISMO' where jne_idhojavida = 133954;
            update candidato set profesion = 'ABOGADO' where jne_idhojavida = 134084;
            update candidato set profesion = 'LICENCIADA EN EDUCACIÓN  INICIAL Y PRIMARIA' where jne_idhojavida = 135058;
            update candidato set profesion = 'ARQUITECTO' where jne_idhojavida = 134780;
            update candidato set profesion = 'ABOGADO' where jne_idhojavida = 134005;
            update candidato set profesion = 'BACHILLER EN CIENCAS DE LA COMUNICACIÓN SOCIAL' where jne_idhojavida = 135769;
            update candidato set profesion = 'TÍTULO PROFESIONAL DE LICENCIADO EN ENFERMERIA,BACHILLER EN ESTOMATOLOGÍA' where jne_idhojavida = 134185;
            update candidato set profesion = 'CONTADOR PÚBLICO' where jne_idhojavida = 134225;
            update candidato set profesion = 'INGENIERO AGRONOMO' where jne_idhojavida = 134011;
            update candidato set profesion = 'LICENCIADA EN CIENCIAS DE LA COMUNICACION' where jne_idhojavida = 134727;
            update candidato set profesion = 'ABOGADO' where jne_idhojavida = 133891;
            update candidato set profesion = 'MÉDICO CIRUJANO' where jne_idhojavida = 133917;
            update candidato set profesion = 'LICENCIADO EN PERIODISMO' where jne_idhojavida = 134125;
            update candidato set profesion = 'LICENCIADO EN ADMINISTRACION' where jne_idhojavida =134171;
            update candidato set profesion = 'ABOGADO' where jne_idhojavida = 134301;
            update candidato set profesion = 'ABOGADO' where jne_idhojavida = 134301;
            update candidato set profesion = 'INGENIERA CIVIL' where jne_idhojavida = 134190;
            update candidato set profesion = 'LICENCIADA EN ENFERMERÍA' where jne_idhojavida = 135958;
            update candidato set profesion = 'ABOGADO, LICENCIADA EN ADMINISTRACIÓN DE EMPRESAS' where jne_idhojavida = 137381;
            update candidato set profesion = 'LICENCIADA EN EDUCACION' where jne_idhojavida = 137477;
            update candidato set profesion = 'TITULO DE SEGUNDA ESPECIALIDAD PROFESIONAL INVESTIGACION Y GESTION EDUCATIVA - UNIVERSITARIO, ABOGADO, LICENCIADO EN EDUCACION ESPECIALIDAD MATEMATICA Y COMPUTACION' where jne_idhojavida = 134929;
            update candidato set profesion = 'LICENCIADO EN EDUCACIÓN SECUNDARIA' where jne_idhojavida = 135579;
            update candidato set profesion = 'LICENCIADA EN OBSTETRICIA' where jne_idhojavida = 134672;
            update candidato set profesion = 'LICENCIADA EN ENFERMERIA' where jne_idhojavida = 133801;
            update candidato set profesion = 'CONTADOR PÚBLICO' where jne_idhojavida = 133912;
            update candidato set profesion = 'LICENCIADO EN EDUCACION' where jne_idhojavida = 133783;
            update candidato set profesion = 'INGENIERO CIVIL' where jne_idhojavida = 134505;
            update candidato set profesion = 'ARQUITECTO' where jne_idhojavida = 134574;
            update candidato set profesion = 'MEDICO CIRUJANO' where jne_idhojavida = 134235;
            update candidato set profesion = 'TITULO PROFESIONAL DE INGENIERO CIVIL' where jne_idhojavida = 134507;
            update candidato set profesion = 'ABOGADA' where jne_idhojavida = 134260;
            update candidato set profesion = 'ABOGADO, LICENCIADO EN ADMINISTRACION Y CIENCIAS POLICIALES, BACHILLER EN EDUCACION' where jne_idhojavida = 134270;
            update candidato set profesion = 'LICENCIADO EN ESTADISTICA' where jne_idhojavida = 133950;
            update candidato set profesion = 'LICENCIADO EN ENFERMERIA' where jne_idhojavida = 134701;
            update candidato set profesion = 'ABOGADO' where jne_idhojavida = 134019;
            update candidato set profesion = 'LICENCIADA EN NEGOCIOS INTERNACIONALES' where jne_idhojavida = 136973;
            update candidato set profesion = 'LICENCIADO EN EDUCACION SECUNDARIA' where jne_idhojavida = 134143;
            update candidato set profesion = 'ABOGADO' where jne_idhojavida = 134373;
            update candidato set profesion = 'LICENCIADA EN PSICOLOGÍA HUMANA' where jne_idhojavida = 134484;
            update candidato set profesion = 'ABOGADO' where jne_idhojavida = 133753;
            update candidato set profesion = 'LICENCIADO EN ADMINISTRACION' where jne_idhojavida = 133763;
            
         """
        cur.execute(query)
        cur.execute(query2)
        con.commit()

        con.close()
        print("Candidato updates success!")
    except (Exception, psycopg2.Error) as error :
        print ("Error while fetching data", error)

    finally:
        if(con):
            cur.close()
            con.close()


if __name__ == "__main__":
    #insert_candidato_target()
    update_candidato_target()