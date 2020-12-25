import crud as cd
import csv

con = cd.connect_postgres()
cur = con.cursor()
with open('../RankingDeUniversidades/RankingUnis.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    count = 0
    for row in reader:
        cur.execute("INSERT INTO public.ranking_universidad( \
            rankingnum, universidadname, presenciaposicion, impactoposicion, aperturaposicion, excelenciaposicion) \
            VALUES(%s, %s, %s, %s, %s, %s)", (int(row['rankingNum']),row['universidadName'],
        int(row['presenciaPosicion']),int(row['impactoPosicion']),
        int(row['aperturaPosicion']),int(row['excelenciaPosicion'])))
        count+=1
        print("insert row ",count," success!")
    con.commit()
con.close()