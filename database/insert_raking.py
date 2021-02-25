import crud as cd
import csv

con = cd.connect_postgres()
cur = con.cursor()



# cur.execute('''
# DROP TABLE IF EXISTS jne.ranking_universidad;
# CREATE TABLE IF NOT EXISTS jne.ranking_universidad(
#     rankingnum int,
#     universidadname character varying,
#     presenciaposicion int,
#     impactoposicion int,
#     aperturaposicion int,
#     excelenciaposicion int
#     );
# ''')
# con.commit()
# print("Table proceso created successfully")
# con.close()


with open('../OthersScrapers/RankingDeUniversidades/RankingUnis.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    count = 0
    for row in reader:
        cur.execute("INSERT INTO jne.ranking_universidad( \
rankingnum, universidadname, presenciaposicion, impactoposicion, aperturaposicion, excelenciaposicion) \
            VALUES(%s, %s, %s, %s, %s, %s)", (int(row['rankingNum']),row['universidadName'],
        int(row['presenciaPosicion']),int(row['impactoPosicion']),
        int(row['aperturaPosicion']),int(row['excelenciaPosicion'])))
        count+=1
        print("insert row ",count," success!")
    con.commit()
con.close()