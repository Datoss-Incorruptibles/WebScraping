import crud as cd
import csv

con = cd.connect_postgres()

cur = con.cursor()
with open('../ConsultaDeListasYCandidatos/Presidentes/DatosPersonales.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    count = 0
    for row in reader:
        cur.execute("INSERT INTO public.candidato(id_proceso, id_candidato, dni, nombre_completo, \
            pos, cargo, jurisdiccion, designado, estado, nombres, apaterno, amaterno, fecha_nac, \
            id_sexo, pais, departamento, provincia, distrito, residencia, correo, registro_org_pol, \
            portal_web, cargo_autoridad, forma_designacion)\
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", 
            (79,int(row['IDCANDIDATO']), row['DNI'], row['NOMBRE_COMPLETO'],int(row['POS']), row['CARGO'],row['JURISDICCIÓN'], 
            row['DESIGNADO'], row['ESTADO'], row['strNombres'],row['strAPaterno'], row['strAMaterno'], row['strFecha_Nac'], 
            int(row['intId_Sexo']), row['strPais'],row['strDepartamento'],row['strProvincia'],row['strDistrito'],
            row['strResidencia'],row['strCorreo'],row['strRegistro_Org_Pol'],row['strPortal_Web'],row['strCargoAutoridad'],
            row['strFormaDesignacion'])
        )
        count+=1
        print("insert row ",count," success!")
    con.commit()


cur = con.cursor()
with open('../ConsultaDeListasYCandidatos/Congresistas/DatosPersonales.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    count = 0
    for row in reader:
        cur.execute("INSERT INTO public.candidato(id_proceso, id_candidato, dni, nombre_completo, \
            pos, cargo, jurisdiccion, designado, estado, nombres, apaterno, amaterno, fecha_nac, \
            id_sexo, pais, departamento, provincia, distrito, residencia, correo, registro_org_pol, \
            portal_web, cargo_autoridad, forma_designacion)\
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", 
            (79,int(row['IDCANDIDATO']), row['DNI'], row['NOMBRE_COMPLETO'],int(row['POS']), row['CARGO'],row['JURISDICCIÓN'], 
            row['DESIGNADO'], row['ESTADO'], row['strNombres'],row['strAPaterno'], row['strAMaterno'], row['strFecha_Nac'], 
            int(row['intId_Sexo']), row['strPais'],row['strDepartamento'],row['strProvincia'],row['strDistrito'],
            row['strResidencia'],row['strCorreo'],row['strRegistro_Org_Pol'],row['strPortal_Web'],row['strCargoAutoridad'],
            row['strFormaDesignacion'])
        )
        count+=1
        print("insert row ",count," success!")
    con.commit()


con.close() 