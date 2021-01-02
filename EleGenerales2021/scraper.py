import requests
from bs4 import BeautifulSoup
import csv
import json
import sys
import datetime

import schedule
import time

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry




def GetExpedientesLista():
  print(f'START getting Data to expedientesLista.json at {datetime.datetime.now()}')

  idPresidentes = 1
  idCongresistas = 2
  idParlamentoAndino = 3
  idProcesoElectoral = 110

  urlCandidatoListarPorID = f'https://plataformaelectoral.jne.gob.pe/Candidato/GetExpedientesLista/{idProcesoElectoral}-{idPresidentes}-------0-'
  r = requests.get(urlCandidatoListarPorID)
  data1 = r.json()
  data1Clean = data1["data"]

  urlCandidatoListarPorID = f'https://plataformaelectoral.jne.gob.pe/Candidato/GetExpedientesLista/{idProcesoElectoral}-{idCongresistas}-------0-'
  r = requests.get(urlCandidatoListarPorID)
  data2 = r.json()
  data2Clean = data2["data"]

  urlCandidatoListarPorID = f'https://plataformaelectoral.jne.gob.pe/Candidato/GetExpedientesLista/{idProcesoElectoral}-{idParlamentoAndino}-------0-'
  r = requests.get(urlCandidatoListarPorID)
  data3 = r.json()
  data3Clean = data3["data"]

  dataAll = data1Clean + data2Clean + data3Clean
  for data in dataAll:
    data["idProcesoElectoral"] = idProcesoElectoral


  with open('./currentRawData/GetExpedientesLista.json', 'w') as json_file:
    json.dump(dataAll, json_file)
  print(f'END getting expedientesLista at :{datetime.datetime.now()}')





def GetCandidatos():
  print(f'Start getting Data to GetCandidatos.json at {datetime.datetime.now()}')

  with open(f'./currentRawData/GetExpedientesLista.json', 'r', encoding='utf-8')as outFile:
    doc = outFile.read()
    # print(doc)
    docString = json.loads(doc)
    # print(docString)

    PARTIDOSPOLITICOSBYREGION = docString
    idProcesoElectoral = 110


  counter = 0
  listaDeCandidatosFinal = []
  PARTIDOSPOLITICOSBYREGION
  for PARTIDO_POLITICO in PARTIDOSPOLITICOSBYREGION:
    urlCandidatosPartidos = f'https://plataformaelectoral.jne.gob.pe/Candidato/GetCandidatos/{PARTIDO_POLITICO["idTipoEleccion"]}-{idProcesoElectoral}-{PARTIDO_POLITICO["idSolicitudLista"]}-{PARTIDO_POLITICO["idExpediente"]}'
    agent = {"User-Agent":"Mozilla/5.0"}
    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)   
    r = session.get(urlCandidatosPartidos, headers=agent)

    responseJSON =r.json()
    # Lista de candidatos por partido y region
    listaDeCantidatos = responseJSON["data"]

    if not len(listaDeCantidatos)> 0:
      print("No hay lista de este partido")
    else:
      for CANDIDATO in listaDeCantidatos:
        try:
          # I need this to make other calls to others endpoints
          CANDIDATO["idOrganizacionPolitica"] = PARTIDO_POLITICO["idOrganizacionPolitica"] 
          CANDIDATO["idProcesoElectoral"] = idProcesoElectoral
          CANDIDATO["idExpediente"] =  PARTIDO_POLITICO["idExpediente"]
          listaDeCandidatosFinal.append(CANDIDATO)
          # print(CANDIDATO)
          # print(CANDIDATO["idOrganizacionPolitica"])
        except:
          print("No hay candidato")
        counter = counter + 1
        # print(counter)

  with open('./currentRawData/GetCandidatos.json', 'w') as json_file:
    json.dump(listaDeCandidatosFinal, json_file)
  
  print(f'End getting GetCandidatos at:{datetime.datetime.now()}')





def GetPersonalData():
  print(f'Start getting Data to CandidatoDatosHV.json at {datetime.datetime.now()}')

  with open(f'./currentRawData/GetCandidatos.json', 'r', encoding='utf-8')as outFile:
    doc = outFile.read()
    arrayCandidatos = json.loads(str(doc))
    canditadosHVDatos = []
    counter = 0
    idHojasDeVida = []

    for CANDIDATO in arrayCandidatos:
      if CANDIDATO["idHojaVida"] in idHojasDeVida:
        print(f'candidato repetido {CANDIDATO["idHojaVida"]}')
      else:
        try:
          urlCandidatoListarPorID = f'https://plataformaelectoral.jne.gob.pe/HojaVida/GetHVConsolidado?param={CANDIDATO["idHojaVida"]}-0-{CANDIDATO["idOrganizacionPolitica"]}-{CANDIDATO["idProcesoElectoral"]}'
          agent = {"User-Agent":"Mozilla/5.0"}
          session = requests.Session()
          retry = Retry(connect=3, backoff_factor=0.5)
          adapter = HTTPAdapter(max_retries=retry)
          session.mount('http://', adapter)
          session.mount('https://', adapter)   
          r = session.get(urlCandidatoListarPorID, headers=agent)

          data = r.json()
          candidatoHV = data["data"]
          counter = counter + 1
          canditadosHVDatos.append(candidatoHV)
          idHojasDeVida.append(CANDIDATO["idHojaVida"])

        except:
          print("CRASH OBTENIENDO LA HOJA DE VIDA")
      
    with open('./currentRawData/CandidatoDatosHV.json', 'w') as json_file:
      json.dump(canditadosHVDatos, json_file)
    print(f'END getting Data to CandidatoDatosHV.json at {datetime.datetime.now()}')



def job():
    print("Job start .... scraper")
    GetExpedientesLista()
    GetCandidatos()
    GetPersonalData()
    print("Job END .... scraper")

# schedule.every(55).minutes.at(":00").do(job)
schedule.every().hour.at(":25").do(job)
# schedule.every().day.at("02:30").do(job)
# schedule.every().minute.at(":50").do(job)

while True:
    schedule.run_pending()
    time.sleep(1)



