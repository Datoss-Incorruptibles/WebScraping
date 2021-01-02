import crud as cd
import json

con = cd.connect_postgres()

cur = con.cursor()
with open('../PlataformaElectoral/EleGenerales2021/CandidatoDatosHV.json','r', encoding='utf-8' ) as jsonfile:
    doc = jsonfile.read()
    arrayCandidatos = json.loads(str(doc))
    count = 0
    cur.execute("TRUNCATE jne.candidato_edu_no_uni;")

    for row in arrayCandidatos:
        row = row["oEduNoUniversitaria"]
        cur.execute( \
          "INSERT INTO jne.candidato_edu_no_uni(idEstado,idHVNoUniversitaria,idHojaVida,strCarreraNoUni,strCentroEstudioNoUni,strConcluidoNoUni,strTengoNoUniversitaria,strUsuario)\
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s)", (
            row["idEstado"],
            row["idHVNoUniversitaria"],
            row["idHojaVida"],
            row["strCarreraNoUni"],
            row["strCentroEstudioNoUni"],
            row["strConcluidoNoUni"],
            row["strTengoNoUniversitaria"],
            row["strUsuario"]
            ))
        count+=1
        con.commit()
        print("insert row ",count," success!")
    con.commit()


print("candidato_edu_no_uni")
