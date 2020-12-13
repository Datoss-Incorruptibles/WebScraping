import requests
from bs4 import BeautifulSoup

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import csv
import json

PARTIDOSPOLITICOS = []
arrayAmbitoCivil = []

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
print(PARTIDOSPOLITICOS)




for PARTIDO_POLITICO in PARTIDOSPOLITICOS:
  urlCandidatosPartidos = f'https://consultalistacandidato.jne.gob.pe/PresidenteCongresoParlamentoAndino/GetCandidatos?IDPROCESO=79&IDEXPEDIENTE={PARTIDO_POLITICO["IDEXPEDIENTE"]}'
  agent = {"User-Agent":"Mozilla/5.0"}

  session = requests.Session()
  retry = Retry(connect=3, backoff_factor=0.5)
  adapter = HTTPAdapter(max_retries=retry)
  session.mount('http://', adapter)
  session.mount('https://', adapter) 
  r = session.get(urlCandidatosPartidos, headers=agent)


  htmlCantidatos = BeautifulSoup(r.text, 'lxml')

  listaDeCantidatos = htmlCantidatos.find('table', attrs={'class':'Candidato'}).find_all('tr')

  XCANDIDATOS = []
  for i in range(len(listaDeCantidatos)): 
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
      XCANDIDATOS.append(CANDIDATOABC)

  PARTIDO_POLITICO["CANDIDATOS"] = XCANDIDATOS

  for CANDIDATO in PARTIDO_POLITICO["CANDIDATOS"]:
    if CANDIDATO["HOJA_VIDA_ENCRIPTADA"] == "VACIO":
      print("NO HAY HOJA DE VIDA DE ESTE CANDIDATO")

    else:
      url = f'https://pecaoe.jne.gob.pe/sipe/HojaVidaEG.aspx?cod={CANDIDATO["HOJA_VIDA_ENCRIPTADA"]}'

      urlCandidatoAmbitoPenal= 'https://pecaoe.jne.gob.pe/servicios/declaracion.asmx/AmbitoCivilListarPorCandidato'
      myBody = {"objCandidatoBE": {"intId_Candidato": CANDIDATO["IDCANDIDATO"], "objProcesoElectoralBE": {"intIdProceso": CANDIDATO["IDPROCESO"]}}}
      # r = requests.post(urlCandidatoAmbitoPenal, json=myBody)
      
      session = requests.Session()
      retry = Retry(connect=3, backoff_factor=0.5)
      adapter = HTTPAdapter(max_retries=retry)
      session.mount('http://', adapter)
      session.mount('https://', adapter)   
      r = session.post(urlCandidatoAmbitoPenal, json=myBody)

      
      
      # print(r)
      # print(r.status_code)
      # print(r.headers['content-type'])
      # print(r.json())
      responseJSON =r.json() 
      datosAmbitoCivil = responseJSON["d"]

      arrayAmbitoCivilCandidatoList = []
      if len(datosAmbitoCivil) > 0:
        for ambCivilCan in datosAmbitoCivil:
            objCandAmbitoCivil = {
                "IDCANDIDATO":CANDIDATO["IDCANDIDATO"],
                "DNI":CANDIDATO["DNI"],
                "NOMBRE_COMPLETO":CANDIDATO["NOMBRE_COMPLETO"],
                "strExpediente":ambCivilCan["strExpediente"],
                "strJuzgado":ambCivilCan["strJuzgado"],
                "strMateria":ambCivilCan["strMateria"],
                "strFallo":ambCivilCan["strFallo"]
            }
            arrayAmbitoCivilCandidatoList.append(objCandAmbitoCivil)
      #Validate if there are no items in the array of the candidate
      if len(arrayAmbitoCivilCandidatoList) > 0:
        arrayAmbitoCivil.append(arrayAmbitoCivilCandidatoList)
# print(arrayAmbitoCivil)



f = csv.writer(open("SentenciasAmbitoCivil.csv", "w", newline=''))
# Write CSV Header, If you dont need that, remove this line
f.writerow(["IDCANDIDATO",
"DNI",
"NOMBRE_COMPLETO",
"strExpediente", 
"strJuzgado",
"strMateria",
"strFallo"
])

for candArrayAmbitoCivil in arrayAmbitoCivil:
  for candAmbitoCivil in candArrayAmbitoCivil:
    f.writerow([
          candAmbitoCivil["IDCANDIDATO"],
          candAmbitoCivil["DNI"],
          candAmbitoCivil["NOMBRE_COMPLETO"],
          candAmbitoCivil["strExpediente"],
          candAmbitoCivil["strJuzgado"],
          candAmbitoCivil["strMateria"],
          candAmbitoCivil["strFallo"]
          ])

