import crud as cd
import csv

con = cd.connect_postgres()

cur = con.cursor()
with open('../ConsultaDeListasYCandidatos/Presidentes/ExperienciaLaboral.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    count = 0
    for row in reader:
        cur.execute("INSERT INTO public.candidato_experiencia(id_proceso, id_candidato, dni, nombre_completo, \
            condicion, empleador, ruc, pais, nombre_sector, cargo, inicioanio, finanio) \
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",  
            (79,int(row['IDCANDIDATO']), row['DNI'], row['NOMBRE_COMPLETO'],int(row['intCondicion']), row['strEmpleador'],
            row['strRuc'], row['strPais'], row['strNombre_Sector'], row['strCargo'],int(row['intInicioAnio']), int(row['intFinAnio']))
        )
        count+=1
        print("process row ",count," success!")
    con.commit()
    print("commited all presidentes rows")


cur = con.cursor()
with open('../ConsultaDeListasYCandidatos/Congresistas/ExperienciaLaboral.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    count = 0
    for row in reader:
        cur.execute("INSERT INTO public.candidato_experiencia(id_proceso, id_candidato, dni, nombre_completo, \
            condicion, empleador, ruc, pais, nombre_sector, cargo, inicioanio, finanio) \
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",  
            (79,int(row['IDCANDIDATO']), row['DNI'], row['NOMBRE_COMPLETO'],int(row['intCondicion']), row['strEmpleador'],
            row['strRuc'], row['strPais'], row['strNombre_Sector'], row['strCargo'],int(row['intInicioAnio']), int(row['intFinAnio']))
        )
        count+=1
        print("process row ",count," success!")
    con.commit()
    print("commited all congresistas rows")


con.close() 