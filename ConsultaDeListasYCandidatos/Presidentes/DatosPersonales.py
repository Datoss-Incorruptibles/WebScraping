import requests
from bs4 import BeautifulSoup
import csv
import json

PARTIDOSPOLITICOS = []
arrayDatosPersonales = []

with open(f'PresidenteCongresoParlamentoAndino.json', 'r', encoding='utf-8')as outFile:
#   print(outFile.read())
    doc = outFile.read()
    docString = json.loads(str(doc))
    # y = json.dumps(outFile)
    # print(docString)
    for partidoPolitico in docString:
        TXORGPOLITICA = partidoPolitico["TXORGPOLITICA"]
        IDEXPEDIENTE = partidoPolitico["IDEXPEDIENTE"]
        OBJPARTIDO = {"TXORGPOLITICA":TXORGPOLITICA, "IDEXPEDIENTE":IDEXPEDIENTE}
        # print(IDEXPEDIENTE,TXORGPOLITICA)
        PARTIDOSPOLITICOS.append(OBJPARTIDO)
print(PARTIDOSPOLITICOS)




for PARTIDO_POLITICO in PARTIDOSPOLITICOS:
  urlCandidatosPartidos = f'https://consultalistacandidato.jne.gob.pe/PresidenteCongresoParlamentoAndino/GetCandidatos?IDPROCESO=79&IDEXPEDIENTE={PARTIDO_POLITICO["IDEXPEDIENTE"]}'
  agent = {"User-Agent":"Mozilla/5.0"}
  r = requests.get(urlCandidatosPartidos, headers=agent)
  htmlCantidatos = BeautifulSoup(r.text, 'lxml')

  listaDeCantidatos = htmlCantidatos.find('table', attrs={'class':'Candidato'}).find_all('tr')

  TRESCANDIDATOS = []
  for i in range(3): 
      try:
        cantidatoOne = listaDeCantidatos[i+1].find_all('td')

        dataExpediente = cantidatoOne[-1].find('a').get('data-expediente')
        dataENCRIPTADA1 = json.loads(dataExpediente)

      except :
        dataENCRIPTADA1 = {'IDORGPOLITICA': "VACIO", 'IDEXPEDIENTE': "VACIO", 'IDCANDIDATO': "VACIO", 'IDPROCESO': "VACIO", 'DATAENCRIPTADA': 'VACIO'}

      cantidatoOneArray = []
      for i in range(len(cantidatoOne)-1): 
          getText = cantidatoOne[i].get_text()
          clean1 = getText.replace('\n','')
          clean2 = clean1.replace('\t','')
          clean3 =  clean2.strip()
          cantidatoOneArray.append(clean3)    

      cantidatoOneArray.append(dataENCRIPTADA1["DATAENCRIPTADA"])
      cantidatoOneArray.append(dataENCRIPTADA1["IDCANDIDATO"])
      cantidatoOneArray.append(dataENCRIPTADA1["IDPROCESO"])

      CANDIDATOABC = {
      "POS":cantidatoOneArray[0],
      "CARGO":cantidatoOneArray[1],
      "DNI":cantidatoOneArray[2],
      "NOMBRE_COMPLETO":cantidatoOneArray[3],
      "NACIMIENTO":cantidatoOneArray[4],
      "GEN":cantidatoOneArray[5],
      "JURISDICCIÓN":cantidatoOneArray[6],
      "DESIGNADO":cantidatoOneArray[7],
      "ESTADO":cantidatoOneArray[8],
      "HOJA_VIDA_ENCRIPTADA": cantidatoOneArray[9],
      "IDCANDIDATO":cantidatoOneArray[10],
      "IDPROCESO":cantidatoOneArray[11]
      }
      TRESCANDIDATOS.append(CANDIDATOABC)

  PARTIDO_POLITICO["CANDIDATOS"] = TRESCANDIDATOS

  for CANDIDATO in PARTIDO_POLITICO["CANDIDATOS"]:
    if CANDIDATO["HOJA_VIDA_ENCRIPTADA"] == "VACIO":
      print("NO HAY HOJA DE VIDA DE ESTE CANDIDATO")

    else:
      urlCandidatoListarPorID = 'https://pecaoe.jne.gob.pe/servicios/declaracion.asmx/CandidatoListarPorID'
      myBody = {"objCandidatoBE": {"intId_Candidato": CANDIDATO["IDCANDIDATO"], "objProcesoElectoralBE": {"intIdProceso": CANDIDATO["IDPROCESO"]}}}
          
          
      r = requests.post(urlCandidatoListarPorID, json=myBody)
      # print(r)
      # print(r.status_code)
      # print(r.headers['content-type'])
      # print(r.json())
      responseJSON =r.json() 
      datosPersonales = responseJSON["d"]
      objDatosPersonales = {
          "IDCANDIDATO":CANDIDATO["IDCANDIDATO"],
          "DNI":CANDIDATO["DNI"],
          "NOMBRE_COMPLETO":CANDIDATO["NOMBRE_COMPLETO"],

          "POS":CANDIDATO["POS"],
          "CARGO":CANDIDATO["CARGO"],
          "JURISDICCIÓN":CANDIDATO["JURISDICCIÓN"],
          "DESIGNADO":CANDIDATO["DESIGNADO"],
          "ESTADO":CANDIDATO["ESTADO"],

          "strAPaterno":datosPersonales["strAPaterno"],
          "strAMaterno":datosPersonales["strAMaterno"],
          "strNombres":datosPersonales["strNombres"],
          "strFecha_Nac":datosPersonales["strFecha_Nac"],
          "intId_Sexo":datosPersonales["intId_Sexo"],
          
              # <b>Lugar de residencia / domicilio</b>

          "strPais":datosPersonales["strPais"],
          "strDepartamento":datosPersonales["objUbigeoResidenciaBE"]["strDepartamento"],
          "strProvincia":datosPersonales["objUbigeoResidenciaBE"]["strProvincia"],
          "strDistrito":datosPersonales["objUbigeoResidenciaBE"]["strDistrito"],
          "strResidencia":datosPersonales["strResidencia"],


          "strCorreo":datosPersonales["strCorreo"],
          "strRegistro_Org_Pol":datosPersonales["strRegistro_Org_Pol"],
          "strPortal_Web":datosPersonales["strPortal_Web"],
          "strCargoAutoridad":datosPersonales["objCargoAutoridadBE"]["strCargoAutoridad"],
          "strFormaDesignacion":datosPersonales["strFormaDesignacion"]
      }

      arrayDatosPersonales.append(objDatosPersonales)



f = csv.writer(open("DatosPersonales.csv", "w", newline=''))

f.writerow(["IDCANDIDATO",
"DNI",
"NOMBRE_COMPLETO",
"POS",
"CARGO",
"JURISDICCIÓN",
"DESIGNADO",
"ESTADO",
"strNombres", 
"strAPaterno",
"strAMaterno",
"strFecha_Nac",
"intId_Sexo",
"strPais",
"strDepartamento",
"strProvincia",
"strDistrito",
"strResidencia",
"strCorreo",
"strRegistro_Org_Pol",
"strPortal_Web",
"strCargoAutoridad",
"strFormaDesignacion"
])

for candidato in arrayDatosPersonales:
    f.writerow([
          candidato["IDCANDIDATO"],
          candidato["DNI"],
          candidato["NOMBRE_COMPLETO"],
          candidato["POS"],
          candidato["CARGO"],
          candidato["JURISDICCIÓN"],
          candidato["DESIGNADO"],
          candidato["ESTADO"],
          candidato["strNombres"],
          candidato["strAPaterno"],
          candidato["strAMaterno"],
          candidato["strFecha_Nac"],
          candidato["intId_Sexo"],
          candidato["strPais"],
          candidato["strDepartamento"],
          candidato["strProvincia"],
          candidato["strDistrito"],
          candidato["strResidencia"],
          candidato["strCorreo"],
          candidato["strRegistro_Org_Pol"],
          candidato["strPortal_Web"],
          candidato["strCargoAutoridad"],
          candidato["strFormaDesignacion"]])
