import crud as cd
import json

con = cd.connect_postgres()

cur = con.cursor()
with open('../PlataformaElectoral/EleGenerales2021/CandidatoDatosHV.json','r', encoding='utf-8' ) as jsonfile:
    doc = jsonfile.read()
    arrayCandidatos = json.loads(str(doc))
    count = 0
    cur.execute("TRUNCATE jne.candidato_edu_basic;")
    for row in arrayCandidatos:
        row = row["oEduBasica"]
        cur.execute( \
          "INSERT INTO jne.candidato_edu_basic(idEstado,idHVEduBasica,idHojaVida,strConcluidoEduPrimaria,strConcluidoEduSecundaria,strEduPrimaria,strEduSecundaria,strTengoEduBasica,strUsuario)\
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)", (
            row["idEstado"],
            row["idHVEduBasica"],
            row["idHojaVida"],
            row["strConcluidoEduPrimaria"],
            row["strConcluidoEduSecundaria"],
            row["strEduPrimaria"],
            row["strEduSecundaria"],
            row["strTengoEduBasica"],
            row["strUsuario"]
            ))
        count+=1
        con.commit()
        print("insert row ",count," success!")
    con.commit()

print("candidato_edu_basic")
