import crud as cd
import json

con = cd.connect_postgres()

cur = con.cursor()
with open('../PlataformaElectoral/EleGenerales2021/CandidatoDatosHV.json','r', encoding='utf-8' ) as jsonfile:
    doc = jsonfile.read()
    arrayCandidatos = json.loads(str(doc))
    count = 0
    cur.execute("TRUNCATE jne.candidato_exp_laboral;")

    for objExp in arrayCandidatos:
      arrayExpLab = objExp["lExperienciaLaboral"]
      for row in arrayExpLab:
        cur.execute( \
          "INSERT INTO jne.candidato_exp_laboral(idEstado,idHojaVida,idHVExpeLaboral,intItemExpeLaboral,strAnioTrabajoDesde,strAnioTrabajoHasta,strCentroTrabajo,strDireccionTrabajo,strOcupacionProfesion,strRucTrabajo,strTengoExpeLaboral,strTrabajoDepartamento,strTrabajoDistrito,strTrabajoPais,strTrabajoProvincia,strUbigeoTrabajo,strUsuario)\
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s)", (
            row["idEstado"],
            row["idHojaVida"],
            row["idHVExpeLaboral"],
            row["intItemExpeLaboral"],
            row["strAnioTrabajoDesde"],
            row["strAnioTrabajoHasta"],
            row["strCentroTrabajo"],
            row["strDireccionTrabajo"],
            row["strOcupacionProfesion"],
            row["strRucTrabajo"],
            row["strTengoExpeLaboral"],
            row["strTrabajoDepartamento"],
            row["strTrabajoDistrito"],
            row["strTrabajoPais"],
            row["strTrabajoProvincia"],
            row["strUbigeoTrabajo"],
            row["strUsuario"]))
        count+=1
        con.commit()
        print("insert row ",count," success!")
    con.commit()

