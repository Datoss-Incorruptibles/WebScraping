import crud as cd
import csv

con = cd.connect_postgres()

cur = con.cursor()
with open('../ConsultaDeListasYCandidatos/Presidentes/EducacionBasica.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    count = 0
    for row in reader:
        cur.execute("INSERT INTO public.candidato_ed_basica(id_proceso, id_candidato, dni, nombre_completo, tipo_educacion, \
            centro_primaria, primaria, ubigeo_primaria_departamento, ubigeo_primaria_provincia, ubigeo_primaria_distrito, \
            anio_inicio_primaria, anio_fin_primaria, centro_secundaria, secundaria, ubigeo_secundaria_departamento, \
            ubigeo_secundaria_provincia, ubigeo_secundaria_distrito, anio_inicio_secundaria, anio_fin_secundaria, pais, gradoi, \
            gradoii, gradoiii, gradoiv, gradov, gradovi) \
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", 
            (79,int(row['IDCANDIDATO']), row['DNI'], row['NOMBRE_COMPLETO'],int(row['intTipoEducacion']), row['strCentroPrimaria'],
            row['strPrimaria'], row['objUbigeoPrimaria_strDepartamento'], row['objUbigeoPrimaria_strProvincia'], row['objUbigeoPrimaria_strDistrito'],
            int(row['intAnioInicioPrimaria']), int(row['intAnioFinPrimaria']), row['strCentroSecundaria'], row['strSecundaria'], 
            row['objUbigeoSecundaria_strDepartamento'],row['objUbigeoSecundaria_strProvincia'],row['objUbigeoSecundaria_strDistrito'],
            int(row['intAnioInicioSecundaria']), int(row['intAnioFinSecundaria']),row['strPais'],row['strGradoI'],row['strGradoII'],
            row['strGradoIII'],row['strGradoIV'], row['strGradoV'],row['strGradoVI'])
        )
        count+=1
        print("insert row ",count," success!")
    con.commit()


cur = con.cursor()
with open('../ConsultaDeListasYCandidatos/Congresistas/EducacionBasica.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    count = 0
    for row in reader:
        cur.execute("INSERT INTO public.candidato_ed_basica(id_proceso, id_candidato, dni, nombre_completo, tipo_educacion, \
            centro_primaria, primaria, ubigeo_primaria_departamento, ubigeo_primaria_provincia, ubigeo_primaria_distrito, \
            anio_inicio_primaria, anio_fin_primaria, centro_secundaria, secundaria, ubigeo_secundaria_departamento, \
            ubigeo_secundaria_provincia, ubigeo_secundaria_distrito, anio_inicio_secundaria, anio_fin_secundaria, pais, gradoi, \
            gradoii, gradoiii, gradoiv, gradov, gradovi) \
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", 
            (79,int(row['IDCANDIDATO']), row['DNI'], row['NOMBRE_COMPLETO'],int(row['intTipoEducacion']), row['strCentroPrimaria'],
            row['strPrimaria'], row['objUbigeoPrimaria_strDepartamento'], row['objUbigeoPrimaria_strProvincia'], row['objUbigeoPrimaria_strDistrito'],
            int(row['intAnioInicioPrimaria']), int(row['intAnioFinPrimaria']), row['strCentroSecundaria'], row['strSecundaria'], 
            row['objUbigeoSecundaria_strDepartamento'],row['objUbigeoSecundaria_strProvincia'],row['objUbigeoSecundaria_strDistrito'],
            int(row['intAnioInicioSecundaria']), int(row['intAnioFinSecundaria']),row['strPais'],row['strGradoI'],row['strGradoII'],
            row['strGradoIII'],row['strGradoIV'], row['strGradoV'],row['strGradoVI'])
        )
        count+=1
        print("insert row ",count," success!")
    con.commit()


con.close() 