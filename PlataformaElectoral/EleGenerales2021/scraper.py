import requests
from bs4 import BeautifulSoup
import csv
import json
import sys

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry



def GetExpedientesLista():
  idProceso = 110
  idPresidentes = 1
  idCongresistas = 2
  idParlamentoAndino = 3

  urlCandidatoListarPorID = f'https://plataformaelectoral.jne.gob.pe/Candidato/GetExpedientesLista/{idProceso}-{idPresidentes}-------0-'
  r = requests.get(urlCandidatoListarPorID)
  data1 = r.json()
  data1Clean = data1["data"]

  urlCandidatoListarPorID = f'https://plataformaelectoral.jne.gob.pe/Candidato/GetExpedientesLista/{idProceso}-{idCongresistas}-------0-'
  r = requests.get(urlCandidatoListarPorID)
  data2 = r.json()
  data2Clean = data2["data"]

  urlCandidatoListarPorID = f'https://plataformaelectoral.jne.gob.pe/Candidato/GetExpedientesLista/{idProceso}-{idParlamentoAndino}-------0-'
  r = requests.get(urlCandidatoListarPorID)
  data3 = r.json()
  data3Clean = data3["data"]

  dataAll = data1Clean + data2Clean + data3Clean
  print("Get Data to expedientesLista.json")

  with open('GetExpedientesLista.json', 'w') as json_file:
    json.dump(dataAll, json_file)

# GetExpedientesLista()




with open(f'GetExpedientesLista.json', 'r', encoding='utf-8')as outFile:
  global PARTIDOSPOLITICOSBYREGION
  doc = outFile.read()
  # print(doc)
  docString = json.loads(doc)
  # print(docString)

  PARTIDOSPOLITICOSBYREGION = docString
  idProcesoElectoral = 110

def GetCandidatos():
  print("Get Data :GetCandidatos.jon")
  counter = 0
  listaDeCandidatosFinal = []
  global PARTIDOSPOLITICOSBYREGION
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
          CANDIDATO["idProceso"] = idProcesoElectoral

          listaDeCandidatosFinal.append(CANDIDATO)
          # print(CANDIDATO)
          # print(CANDIDATO["idOrganizacionPolitica"])
        except:
          print("No hay candidato")
        counter = counter + 1
        print(counter)

  with open('GetCandidatos.json', 'w') as json_file:
    json.dump(listaDeCandidatosFinal, json_file)
  
# GetCandidatos()



with open(f'GetCandidatos.json', 'r', encoding='utf-8')as outFile:
  doc = outFile.read()
  arrayCandidatos = json.loads(str(doc))
  # print(arrayCandidatos)

def GetPersonalData():
  canditadosHVDatos = []
  counter = 0
  for CANDIDATO in arrayCandidatos:
      try:
        urlCandidatoListarPorID = f'https://plataformaelectoral.jne.gob.pe/HojaVida/GetHVConsolidado?param={CANDIDATO["idHojaVida"]}-0-{CANDIDATO["idOrganizacionPolitica"]}-{CANDIDATO["idProceso"]}'
        r = requests.get(urlCandidatoListarPorID)
        data = r.json()
        candidatoHV = data["data"]
        counter = counter + 1
        print(counter)
        canditadosHVDatos.append(candidatoHV)

      except:
        print("CRASH OBTENIENDO LA HOJA DE VIDA")
    
  with open('CandidatoDatosHV.json', 'w') as json_file:
    json.dump(canditadosHVDatos, json_file)


# GetPersonalData()
