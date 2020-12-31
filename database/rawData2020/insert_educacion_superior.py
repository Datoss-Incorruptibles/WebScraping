import crud as cd
import csv

con = cd.connect_postgres()

cur = con.cursor()
with open('../ConsultaDeListasYCandidatos/Presidentes/EducacionSuperior.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    count = 0
    for row in reader:
        cur.execute("INSERT INTO public.candidato_ed_superior(id_proceso, id_candidato, dni, nombre_completo, tipo_estudio, \
            nombre_carrera, nombre_estudio, nombre_centro, concluido, tipogrado, otro_tipo_grado, anio_inicio, anio_final) \
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",  
            (79,int(row['IDCANDIDATO']), row['DNI'], row['NOMBRE_COMPLETO'],int(row['objTipoEstudioBE_intTipo']), row['strNombreCarrera'],
            row['strNombreEstudio'], row['strNombreCentro'], bool(row['strFgConcluido']), row['strTipoGrado'],row['strOtroTipoGrado'],
            int(row['intAnioInicio']), int(row['intAnioFinal']))
        )
        count+=1
        print("process row ",count," success!")
    con.commit()
    print("commited all presidentes rows")


cur = con.cursor()
with open('../ConsultaDeListasYCandidatos/Congresistas/EducacionSuperior.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    count = 0
    for row in reader:
        cur.execute("INSERT INTO public.candidato_ed_superior(id_proceso, id_candidato, dni, nombre_completo, tipo_estudio, \
            nombre_carrera, nombre_estudio, nombre_centro, concluido, tipogrado, otro_tipo_grado, anio_inicio, anio_final) \
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",  
            (79,int(row['IDCANDIDATO']), row['DNI'], row['NOMBRE_COMPLETO'],int(row['objTipoEstudioBE_intTipo']), row['strNombreCarrera'],
            row['strNombreEstudio'], row['strNombreCentro'], bool(row['strFgConcluido']), row['strTipoGrado'],row['strOtroTipoGrado'],
            int(row['intAnioInicio']), int(row['intAnioFinal']))
        )
        count+=1
        print("process row ",count," success!")
    con.commit()
    print("commited all congresistas rows")


con.close() 