import requests

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from bs4 import BeautifulSoup
import csv
import json



PARTIDOSPOLITICOS = []
arrayCandidatosEducacionSuperior = []

with open(f'Congresistas.json', 'r', encoding='utf-8')as outFile:
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
# print(PARTIDOSPOLITICOS)


for PARTIDO_POLITICO in PARTIDOSPOLITICOS:
  urlCandidatosPartidos = f'https://consultalistacandidato.jne.gob.pe/PresidenteCongresoParlamentoAndino/GetCandidatos?IDPROCESO=79&IDEXPEDIENTE={PARTIDO_POLITICO["IDEXPEDIENTE"]}'
  agent = {"User-Agent":"Mozilla/5.0"}
  r = requests.get(urlCandidatosPartidos, headers=agent)
  htmlCantidatos = BeautifulSoup(r.text, 'lxml')

  listaDeCantidatos = htmlCantidatos.find('table', attrs={'class':'Candidato'}).find_all('tr')

  XCANDIDATOS = []
  for i in range(len(listaDeCantidatos)): 
      # print(cantidatoOne)
      try:
        cantidatoOne = listaDeCantidatos[i+1].find_all('td')

        dataExpediente = cantidatoOne[-1].find('a').get('data-expediente')
        dataENCRIPTADA1 = json.loads(dataExpediente)

      except :
        dataENCRIPTADA1 = {'IDORGPOLITICA': "VACIO", 'IDEXPEDIENTE': "VACIO", 'IDCANDIDATO': "VACIO", 'IDPROCESO': "VACIO", 'DATAENCRIPTADA': 'VACIO'}

      # dataENCRIPTADA1 = dataENCRIPTADA.replace('"',"'")
      # dataENCRIPTADA2 = dataENCRIPTADA1.replace('}','')
      # print(dataExpediente)
      # print(dataENCRIPTADA1["DATAENCRIPTADA"])
      # print(dataENCRIPTADA1)
      cantidatoOneArray = []
      for i in range(len(cantidatoOne)-1): 
          getText = cantidatoOne[i].get_text()
          clean1 = getText.replace('\n','')
          clean2 = clean1.replace('\t','')
          clean3 =  clean2.strip()
          cantidatoOneArray.append(clean3)    
          # cantidatoOneArray.append(cantidatoOne[i].get_text())
      # cantidatoOneArray.append(dataENCRIPTADA)
      cantidatoOneArray.append(dataENCRIPTADA1["DATAENCRIPTADA"])
      cantidatoOneArray.append(dataENCRIPTADA1["IDCANDIDATO"])
      cantidatoOneArray.append(dataENCRIPTADA1["IDPROCESO"])

      # print(cantidatoOneArray[9])
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
      XCANDIDATOS.append(CANDIDATOABC)

  PARTIDO_POLITICO["CANDIDATOS"] = XCANDIDATOS

  for CANDIDATO in PARTIDO_POLITICO["CANDIDATOS"]:
    if CANDIDATO["HOJA_VIDA_ENCRIPTADA"] == "VACIO":
      print("NO HAY HOJA DE VIDA DE ESTE CANDIDATO")

    else:
      
      urlEducacionSuperior = 'https://pecaoe.jne.gob.pe/servicios/declaracion.asmx/EducacionSuperiorListarPorCandidato'
      myBody = {"objCandidatoBE": {"intId_Candidato": CANDIDATO["IDCANDIDATO"], "objProcesoElectoralBE": {"intIdProceso": CANDIDATO["IDPROCESO"]}}}

      session = requests.Session()
      retry = Retry(connect=3, backoff_factor=0.5)
      adapter = HTTPAdapter(max_retries=retry)
      session.mount('http://', adapter)
      session.mount('https://', adapter)   
      r = session.post(urlEducacionSuperior, json=myBody)

      responseJSON =r.json() 

      len(responseJSON["d"])
      arrayEducacionSuperior = []

      for CandEducacionSuperior in responseJSON["d"]:
          objCandEducacionSuperior = {
              "IDCANDIDATO":CANDIDATO["IDCANDIDATO"],
              "DNI":CANDIDATO["DNI"],
              "NOMBRE_COMPLETO":CANDIDATO["NOMBRE_COMPLETO"],
              "objTipoEstudioBE_intTipo":CandEducacionSuperior["objTipoEstudioBE"]["intTipo"],  # 4 PostGrado # 3 Universitario # 1 Tecnico  
              "strNombreCarrera":CandEducacionSuperior["strNombreCarrera"], 
              "strNombreEstudio":CandEducacionSuperior["strNombreEstudio"],
              "strNombreCentro":CandEducacionSuperior["strNombreCentro"],
              "strFgConcluido":CandEducacionSuperior["strFgConcluido"], # 1 concluido
              "strTipoGrado":CandEducacionSuperior["strTipoGrado"],
              "strOtroTipoGrado":CandEducacionSuperior["strOtroTipoGrado"],
              "intAnioInicio":CandEducacionSuperior["intAnioInicio"],
              "intAnioFinal":CandEducacionSuperior["intAnioFinal"]

          }
          arrayEducacionSuperior.append(objCandEducacionSuperior)
      arrayCandidatosEducacionSuperior.append(arrayEducacionSuperior)   
          

# print(arrayCandidatosEducacionSuperior)
print("finish")


# f = csv.writer(open("test.csv", "wb+"))
f = csv.writer(open("EducacionSuperior.csv", "w", newline=''))
# Write CSV Header, If you dont need that, remove this line
f.writerow(["IDCANDIDATO",
"DNI",
"NOMBRE_COMPLETO",
"objTipoEstudioBE_intTipo", 
"strNombreCarrera",
"strNombreEstudio",
"strNombreCentro",
"strFgConcluido",
"strTipoGrado",
"strOtroTipoGrado",
"intAnioInicio",
"intAnioFinal"
])

for candidatoEduSuperiorS in arrayCandidatosEducacionSuperior:
  for candidatoEduSuperior in candidatoEduSuperiorS:
    f.writerow([
          candidatoEduSuperior["IDCANDIDATO"],
          candidatoEduSuperior["DNI"],
          candidatoEduSuperior["NOMBRE_COMPLETO"],
          candidatoEduSuperior["objTipoEstudioBE_intTipo"],
          candidatoEduSuperior["strNombreCarrera"],
          candidatoEduSuperior["strNombreEstudio"],
          candidatoEduSuperior["strNombreCentro"],
          candidatoEduSuperior["strFgConcluido"],
          candidatoEduSuperior["strTipoGrado"],
          candidatoEduSuperior["strOtroTipoGrado"],
          candidatoEduSuperior["intAnioInicio"],
          candidatoEduSuperior["intAnioFinal"]
          ])

