import crud as cd
import json

con = cd.connect_postgres()

cur = con.cursor()
with open('../PlataformaElectoral/EleGenerales2021/CandidatoDatosHV.json','r', encoding='utf-8' ) as jsonfile:
    doc = jsonfile.read()
    arrayCandidatos = json.loads(str(doc))
    count = 0
    cur.execute("TRUNCATE jne.candidato_edu_tecnica;")

    for row in arrayCandidatos:
        row = row["oEduTecnico"]
        if row:
          cur.execute( \
            "INSERT INTO jne.candidato_edu_tecnica(idEstado,idHojaVida,idHVEduTecnico,strCarreraTecnico,strCenEstudioTecnico,strConcluidoEduTecnico,strTengoEduTecnico,strUsuario)\
              VALUES(%s, %s, %s, %s, %s, %s, %s, %s)", (
              row["idEstado"],
              row["idHojaVida"],
              row["idHVEduTecnico"],
              row["strCarreraTecnico"],
              row["strCenEstudioTecnico"],
              row["strConcluidoEduTecnico"],
              row["strTengoEduTecnico"],
              row["strUsuario"]
              ))
          count+=1
          con.commit()
          print("insert row ",count," success!")
    con.commit()

print("candidato_edu_tecnica")
