import crud as cd
import json

con = cd.connect_postgres()

cur = con.cursor()
with open('../PlataformaElectoral/EleGenerales2021/CandidatoDatosHV.json','r', encoding='utf-8' ) as jsonfile:
    doc = jsonfile.read()
    arrayCandidatos = json.loads(str(doc))
    count = 0
    cur.execute("TRUNCATE jne.candidato_sent_civil;")

    for obj in arrayCandidatos:
      listSentObl = obj["lSentenciaObliga"]
      for row in listSentObl:
        cur.execute( \
          "INSERT INTO jne.candidato_sent_civil(idEstado,idHVSentenciaObliga,idHojaVida,idParamMateriaSentencia,intItemSentenciaObliga,strEstado,strExpedienteObliga,strFalloObliga,strMateriaSentencia,strOrder,strOrganoJuridicialObliga,strTengoSentenciaObliga,strUsuario)\
            VALUES( %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s)", (
            row["idEstado"],
            row["idHVSentenciaObliga"],
            row["idHojaVida"],
            row["idParamMateriaSentencia"],
            row["intItemSentenciaObliga"],
            row["strEstado"],
            row["strExpedienteObliga"],
            row["strFalloObliga"],
            row["strMateriaSentencia"],
            row["strOrder"],
            row["strOrganoJuridicialObliga"],
            row["strTengoSentenciaObliga"],
            row["strUsuario"]
            ))
        count+=1
        con.commit()
        print("insert row ",count," success!")
    con.commit()

