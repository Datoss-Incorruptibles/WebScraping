import crud as cd
import json

con = cd.connect_postgres()

cur = con.cursor()
with open('../PlataformaElectoral/EleGenerales2021/CandidatoDatosHV.json','r', encoding='utf-8' ) as jsonfile:
    doc = jsonfile.read()
    arrayCandidatos = json.loads(str(doc))
    count = 0
    cur.execute("TRUNCATE jne.candidato_edu_uni;")

    for obj in arrayCandidatos:
      objPosGrado = obj["oEduPosgrago"]
      arrayData = obj["lEduUniversitaria"]
      for row in arrayData:
        cur.execute( \
          "INSERT INTO jne.candidato_edu_uni(idEstado,idHojaVida,idHVEduUniversitaria,intItemEduUni,strAnioBachiller,strAnioTitulo,strBachillerEduUni,strCarreraUni,strConcluidoEduUni,strEduUniversitaria,strEgresadoEduUni,strMetodoAccion,strOrder,strTengoEduUniversitaria,strTituloUni,strUniversidad,strUsuario)\
            VALUES( %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s)", (
            row["idEstado"],
            objPosGrado["idHojaVida"],
            row["idHVEduUniversitaria"],
            row["intItemEduUni"],
            row["strAnioBachiller"],
            row["strAnioTitulo"],
            row["strBachillerEduUni"],
            row["strCarreraUni"],
            row["strConcluidoEduUni"],
            row["strEduUniversitaria"],
            row["strEgresadoEduUni"],
            row["strMetodoAccion"],
            row["strOrder"],
            row["strTengoEduUniversitaria"],
            row["strTituloUni"],
            row["strUniversidad"],
            row["strUsuario"]))
        count+=1
        con.commit()
        print("insert row ",count," success!")
    con.commit()
