import crud as cd
import json

con = cd.connect_postgres()

cur = con.cursor()

with open('../PlataformaElectoral/EleGenerales2021/CandidatoDatosHV.json','r', encoding='utf-8' ) as jsonfile:
    doc = jsonfile.read()
    arrayCandidatos = json.loads(str(doc))
    count = 0

    cur.execute("TRUNCATE jne.candidato_info_personal;")
    for row in arrayCandidatos:
        row = row["oDatosPersonales"]
        cur.execute( \
          "INSERT INTO jne.candidato_info_personal(strDocumentoIdentidad,idCandidato,idCargoEleccion,idEstado,idHojaVida,idOrganizacionPolitica,idParamHojaVida,idProcesoElectoral,idTipoEleccion,strAnioPostula,strApellidoMaterno,strApellidoPaterno,strCargoEleccion,strCarneExtranjeria,strClase,strDomiDepartamento,strDomiDistrito,strDomiProvincia,strDomicilioDirecc,strEstado,strFeTerminoRegistro,strFechaNacimiento,strHojaVida,strNaciDepartamento,strNaciDistrito,strNaciProvincia,strNombres,strPaisNacimiento,strPostulaDepartamento,strPostulaDistrito,strPostulaProvincia,strProcesoElectoral,strRutaArchivo,strSexo,strUbigeoDomicilio,strUbigeoNacimiento,strUbigeoPostula,strUsuario)\
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s)", (
            row["strDocumentoIdentidad"],
            row["idCandidato"],
            row["idCargoEleccion"],
            row["idEstado"],
            row["idHojaVida"],
            row["idOrganizacionPolitica"],
            row["idParamHojaVida"],
            row["idProcesoElectoral"],
            row["idTipoEleccion"],
            row["strAnioPostula"],
            row["strApellidoMaterno"],
            row["strApellidoPaterno"],
            row["strCargoEleccion"],
            row["strCarneExtranjeria"],
            row["strClase"],
            row["strDomiDepartamento"],
            row["strDomiDistrito"],
            row["strDomiProvincia"],
            row["strDomicilioDirecc"],
            row["strEstado"],
            row["strFeTerminoRegistro"],
            row["strFechaNacimiento"],
            row["strHojaVida"],
            row["strNaciDepartamento"],
            row["strNaciDistrito"],
            row["strNaciProvincia"],
            row["strNombres"],
            row["strPaisNacimiento"],
            row["strPostulaDepartamento"],
            row["strPostulaDistrito"],
            row["strPostulaProvincia"],
            row["strProcesoElectoral"],
            row["strRutaArchivo"],
            row["strSexo"],
            row["strUbigeoDomicilio"],
            row["strUbigeoNacimiento"],
            row["strUbigeoPostula"],
            row["strUsuario"]))
        count+=1
        # con.commit()
        print("insert row ",count," success!")
    con.commit()

