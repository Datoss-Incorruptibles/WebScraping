import psycopg2
from database import connect_db

def insert_organizacion_target():
    try: 
        con = connect_db()
        cur = con.cursor()

        query = """
            INSERT INTO public.organizacion_politica(nombre, fundacion_fecha, estado, 
            descripcion, ruta_archivo, jne_idorganizacionpolitica, url) 
	        select strorganizacionpolitica, null, case when sum(estado) > 0 then 1 else 0 end,'<h1>Partido politico</h1><p>historia en breve</p>',
            CONCAT('https://aplicaciones007.jne.gob.pe/srop_publico/Consulta/Simbolo/GetSimbolo/', idorganizacionpolitica),
            idorganizacionpolitica, '' 

            from (
                SELECT idorganizacionpolitica, strorganizacionpolitica,
                case when opr.strestadolista not in ('INADMISIBLE', 'IMPROCEDENTE') then 1 else 0 end estado
                from jne.organizacion_politica_region opr
                group by idorganizacionpolitica, strorganizacionpolitica, opr.strestadolista
            order by 1,2,3
            ) a group by idorganizacionpolitica, strorganizacionpolitica;
        """

        query_update = """
            update organizacion_politica set fundacion_fecha = cast ( '1924-05-07' as date) , url = 'https://apraperu.com/' where jne_idorganizacionpolitica =32;
            update organizacion_politica set fundacion_fecha = cast ( '1956-07-07' as date), url= 'https://www.accionpopular.pe/' where jne_idorganizacionpolitica =4;
            update organizacion_politica set fundacion_fecha = cast ( '1966-12-18' as date), url= 'http://ppc.pe/' where jne_idorganizacionpolitica =15;
            update organizacion_politica set fundacion_fecha = cast ( '2013-1-21' as date) , url= 'https://www.partidoruna.pe/	' where jne_idorganizacionpolitica =5;
            update organizacion_politica set fundacion_fecha = cast ( '2001-11-17' as date) , url= 'http://www.democraciadirecta.pe' where jne_idorganizacionpolitica =2191;
            update organizacion_politica set fundacion_fecha = cast ( '2001-12-08' as date) , url= 'https://app.pe/' where jne_idorganizacionpolitica =1257;
            update organizacion_politica set fundacion_fecha = cast ( '2005-11-25' as date) , url= 'www.restauracionnacional.org.pe' where jne_idorganizacionpolitica =21;
            update organizacion_politica set fundacion_fecha = cast ( '2018-10-01' as date) , url= 'https://podemosperu.pe/' where jne_idorganizacionpolitica =2731;
            update organizacion_politica set fundacion_fecha = cast ( '1989-09-30' as date) , url= 'https://frepap.org.pe/' where jne_idorganizacionpolitica =2646;
            update organizacion_politica set fundacion_fecha = cast ( '2012-02-15' as date), url= 'https://www.facebook.com/Partido-Politico-Nacional-PER%C3%9A-LIBRE-1834125436815190/' where jne_idorganizacionpolitica =2218;
            update organizacion_politica set fundacion_fecha = cast ( '2013-06-21' as date), url= 'http://frenteamplioperu.pe/' where jne_idorganizacionpolitica =2160;
            update organizacion_politica set fundacion_fecha = cast ( '2000-04-10' as date), url= 'http://www.avanzapais.org.pe/' where jne_idorganizacionpolitica =2173;
            update organizacion_politica set fundacion_fecha = cast ( '1997-06-01' as date), url= 'http://www.somosperu.pe/' where jne_idorganizacionpolitica =14;
            update organizacion_politica set fundacion_fecha = cast ( '2012-09-16' as date), url= 'https://www.perupatriasegura.org/' where jne_idorganizacionpolitica =55;
            update organizacion_politica set fundacion_fecha = cast ( '2010-03-09' as date), url= 'www.fuerza2011.com/' where jne_idorganizacionpolitica =1366;
            update organizacion_politica set fundacion_fecha = cast ( '2017-11-18' as date), url= 'https://www.partidomorado.pe/' where jne_idorganizacionpolitica =2840;
            update organizacion_politica set fundacion_fecha = cast ( '2020/10/30' as date), url= 'https://www.facebook.com/PartidoFrentedelaEsperanza2021/' where jne_idorganizacionpolitica =2857;
            update organizacion_politica set fundacion_fecha = cast ( '2005-10-03' as date), url= 'https://www.partidonacionalistaperuano.org.pe/' where jne_idorganizacionpolitica =179;
            update organizacion_politica set fundacion_fecha = cast ( '2020-10-07' as date), url= 'www.solidaridadnacional.pe' where jne_idorganizacionpolitica =22;
            update organizacion_politica set fundacion_fecha = cast ( '1994-09-21' as date), url= 'https://www.upp.pe/' where jne_idorganizacionpolitica =47;
            update organizacion_politica set fundacion_fecha = cast ( '2017-5-22' as date), url= 'www.partidohumanista.org.pe/' where jne_idorganizacionpolitica =1264;
            update organizacion_politica set fundacion_fecha = cast ( '2012-07-24' as date), url= 'Http://partidocontigo.pe/' where jne_idorganizacionpolitica =2235;
            update organizacion_politica set fundacion_fecha = cast ( '2020-10-30' as date), url= 'https://www.facebook.com/PartidoFrentedelaEsperanza2021/' where jne_idorganizacionpolitica =2857;
        """
        cur.execute(query)
        cur.execute(query_update)
        con.commit()
        con.close()
        print ("Organizacion Politica inserts success!")
    except (Exception, psycopg2.Error) as error :
        print ("Error while fetching data", error)

    finally:
        if(con):
            cur.close()
            con.close()

if __name__ == "__main__":
    insert_organizacion_target()