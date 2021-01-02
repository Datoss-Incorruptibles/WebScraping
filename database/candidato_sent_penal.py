import crud as cd
import json

con = cd.connect_postgres()

cur = con.cursor()
with open('../PlataformaElectoral/EleGenerales2021/CandidatoDatosHV.json','r', encoding='utf-8' ) as jsonfile:
    doc = jsonfile.read()
    arrayCandidatos = json.loads(str(doc))
    count = 0

    cur.execute("TRUNCATE jne.candidato_sent_penal;")

    for obj in arrayCandidatos:
      listSentPenal = obj["lSentenciaPenal"]
      for row in listSentPenal:
        cur.execute(
          "INSERT INTO jne.candidato_sent_penal(idEstado,idHVSentenciaPenal,idHojaVida,idParamCumpleFallo,idParamModalidad,intItemSentenciaPenal,strCumpleFallo,strDelitoPenal,strExpedientePenal,strFalloPenal,strFechaSentenciaPenal,strModalidad,strOrder,strOrganoJudiPenal,strOtraModalidad,strTengoSentenciaPenal,strUsuario)\
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s)", (
            row["idEstado"],
            row["idHVSentenciaPenal"],
            row["idHojaVida"],
            row["idParamCumpleFallo"],
            row["idParamModalidad"],
            row["intItemSentenciaPenal"],
            row["strCumpleFallo"],
            row["strDelitoPenal"],
            row["strExpedientePenal"],
            row["strFalloPenal"],
            row["strFechaSentenciaPenal"],
            row["strModalidad"],
            row["strOrder"],
            row["strOrganoJudiPenal"],
            row["strOtraModalidad"],
            row["strTengoSentenciaPenal"],
            row["strUsuario"]
            ))
        count+=1
        con.commit()
        print("insert row ",count," success!")
    con.commit()

