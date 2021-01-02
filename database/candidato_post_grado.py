import crud as cd
import json

con = cd.connect_postgres()

cur = con.cursor()
with open('../PlataformaElectoral/EleGenerales2021/CandidatoDatosHV.json','r', encoding='utf-8' ) as jsonfile:
    doc = jsonfile.read()
    arrayCandidatos = json.loads(str(doc))
    count = 0
    cur.execute("TRUNCATE jne.candidato_post_grado;")

    for row in arrayCandidatos:
        row = row["oEduPosgrago"]
        cur.execute( \
          "INSERT INTO jne.candidato_post_grado(idEstado,idHojaVida,idHVPosgrado,intItemPosgrado,strAnioPosgrado,strCenEstudioPosgrado,strConcluidoPosgrado,strEgresadoPosgrado,strEsDoctor,strEsMaestro,strEspecialidadPosgrado,strTengoPosgrado,strUsuario)\
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s)", (
            row["idEstado"],
            row["idHojaVida"],
            row["idHVPosgrado"],
            row["intItemPosgrado"],
            row["strAnioPosgrado"],
            row["strCenEstudioPosgrado"],
            row["strConcluidoPosgrado"],
            row["strEgresadoPosgrado"],
            row["strEsDoctor"],
            row["strEsMaestro"],
            row["strEspecialidadPosgrado"],
            row["strTengoPosgrado"],
            row["strUsuario"]))
        count+=1
        con.commit()
        print("insert row ",count," success!")
    con.commit()

