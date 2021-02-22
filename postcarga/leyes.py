import psycopg2
from database import connect_db


def normalizeLeyesCongreso():
    try: 
        con = connect_db()
        cur = con.cursor()
# -- Agregar campo en tabla
        query1 = """
                    alter table de.proyectos_ley_autores add column jne_idhojavida integer
                  """

# -- Actualizar id de proyectos de ley autores
        query2 = """
            update de.proyectos_ley_autores pla set jne_idhojavida = ne.jne_idhojavida
            from
            (			
			select ca.jne_idhojavida,ca.apellido_paterno, ca.apellido_materno, ca.nombres,  pla.nombre , count(*)
			from de.proyectos_ley_autores pla 
			join candidato ca on trim(SP_ASCII(ca.apellido_paterno || ' ' ||  ca.apellido_materno || '  ' || ca.nombres )) = UPPER(trim(SP_ASCII(pla.nombre)))
			 group by 1,2,3,4,5					
            ) ne
            where  pla.nombre = ne.nombre
      """

#  -- agregar a mano adicionales
        query3 = """
            update de.proyectos_ley_autores pla set jne_idhojavida = ne.jne_idhojavida
            from (	
 			select ca.jne_idhojavida,ca.apellido_paterno, ca.apellido_materno, ca.nombres,  pla.nombre , count(*)
			from de.proyectos_ley_autores pla 
			join candidato ca on ca.apellido_paterno = 'RIVAS' AND CA.APELLIDO_MATERNO = 'TEIXEIRA'
			AND  UPPER(trim(SP_ASCII(pla.nombre))) LIKE '%TEXEIRA%'
			 group by 1,2,3,4,5		
             ) ne where  pla.nombre = ne.nombre;
			 """
			 

       """
			update de.proyectos_ley_autores pla set jne_idhojavida = ne.jne_idhojavida
            from (	
 			select ca.jne_idhojavida,ca.apellido_paterno, ca.apellido_materno, ca.nombres,  pla.nombre , count(*)
			from de.proyectos_ley_autores pla 
			join candidato ca on ca.apellido_paterno = 'OMONTE' AND CA.APELLIDO_MATERNO = 'DURAND'
			AND  UPPER(trim(SP_ASCII(pla.nombre))) LIKE '%OMONTE%'
			 group by 1,2,3,4,5		
             ) ne where  pla.nombre = ne.nombre;

 			update de.proyectos_ley_autores pla set jne_idhojavida = ne.jne_idhojavida
            from (	
 			select ca.jne_idhojavida,ca.apellido_paterno, ca.apellido_materno, ca.nombres,  pla.nombre , count(*)
			from de.proyectos_ley_autores pla 
			join candidato ca on ca.apellido_paterno = 'ESCALANTE' AND CA.APELLIDO_MATERNO = 'LEON'
			AND  UPPER(trim(SP_ASCII(pla.nombre))) LIKE '%ESCALANTE%'
			 group by 1,2,3,4,5		
             ) ne where  pla.nombre = ne.nombre;
			 
			 
 			update de.proyectos_ley_autores pla set jne_idhojavida = ne.jne_idhojavida
            from (	
 			select ca.jne_idhojavida,ca.apellido_paterno, ca.apellido_materno, ca.nombres,  pla.nombre , count(*)
			from de.proyectos_ley_autores pla 
			join candidato ca on ca.apellido_paterno = 'BELMONT' AND CA.APELLIDO_MATERNO = 'CASSINELLI'
			AND  UPPER(trim(SP_ASCII(pla.nombre))) LIKE '%BELMONT%'
			 group by 1,2,3,4,5		
             ) ne where  pla.nombre = ne.nombre;
			 
			 			 
 			update de.proyectos_ley_autores pla set jne_idhojavida = ne.jne_idhojavida
            from (	
 			select ca.jne_idhojavida,ca.apellido_paterno, ca.apellido_materno, ca.nombres,  pla.nombre , count(*)
			from de.proyectos_ley_autores pla 
			join candidato ca on ca.apellido_paterno = 'IBERICO' AND CA.APELLIDO_MATERNO = 'NUÑEZ'
			AND  UPPER(trim(SP_ASCII(pla.nombre))) LIKE '%IBERICO%'
			 group by 1,2,3,4,5		
             ) ne where  pla.nombre = ne.nombre;
			 
			update de.proyectos_ley_autores pla set jne_idhojavida = ne.jne_idhojavida
            from (	
 			select ca.jne_idhojavida,ca.apellido_paterno, ca.apellido_materno, ca.nombres,  pla.nombre , count(*)
			from de.proyectos_ley_autores pla 
			join candidato ca on ca.apellido_paterno = 'DIAZ' AND CA.APELLIDO_MATERNO = 'PERALTA' AND NOMBRES = 'GILBERTO LORENZO'
			AND UPPER(trim(SP_ASCII(pla.nombre))) LIKE 'DIAZ PERALTA  GILBERTO'
			 group by 1,2,3,4,5		
             ) ne where  pla.nombre = ne.nombre;
			 
			 	update de.proyectos_ley_autores pla set jne_idhojavida = ne.jne_idhojavida
            from (	
 			select ca.jne_idhojavida,ca.apellido_paterno, ca.apellido_materno, ca.nombres,  pla.nombre , count(*)
			from de.proyectos_ley_autores pla 
			join candidato ca on ca.apellido_paterno = 'REGGIARDO' AND CA.APELLIDO_MATERNO = 'SAYAN'  
			AND UPPER(trim(SP_ASCII(pla.nombre))) LIKE '%REGGIARDO SAYAN%'
			 group by 1,2,3,4,5		
             ) ne where  pla.nombre = ne.nombre;
			 
			 update de.proyectos_ley_autores pla set jne_idhojavida = ne.jne_idhojavida
            from (	
 			select ca.jne_idhojavida,ca.apellido_paterno, ca.apellido_materno, ca.nombres,  pla.nombre , count(*)
			from de.proyectos_ley_autores pla 
			join candidato ca on ca.apellido_paterno = 'FIGUEROA' AND CA.APELLIDO_MATERNO = 'VIZCARRA'  
			AND UPPER(trim(SP_ASCII(pla.nombre))) LIKE '%FIGUEROA VIZCARRA%'
			 group by 1,2,3,4,5		
             ) ne where  pla.nombre = ne.nombre;
        """

        cur.execute(query1)
        con.commit()

        cur.execute(query2)
        con.commit()

        cur.execute(query3)
        con.commit()
        con.close()
        print("Medios normalize  success!")
    except (Exception, psycopg2.Error) as error :
        print ("Error while fetching data", error)

    finally:
        if(con):
            cur.close()
            con.close()


if __name__ == "__main__":
    normalizeLeyesCongreso()