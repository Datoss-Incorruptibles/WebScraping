import requests
from bs4 import BeautifulSoup
import csv
import json
import sys

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry



# def GetExpedienteLista():
#   print("Get Data to expedientesLista.json")


with open(f'expedientesLista.json', 'r', encoding='utf-8')as outFile:
  global PARTIDOSPOLITICOS
  doc = outFile.read()
  docString = json.loads(str(doc))
  PARTIDOSPOLITICOS = docString["data"]

# def GetCandidatos():
#   print("Get Data :GetCandidatos.jon")
#   counter = 0
#   listaDeCandidatos = []
#   global PARTIDOSPOLITICOS
#   for PARTIDO_POLITICO in PARTIDOSPOLITICOS:
#     urlCandidatosPartidos = f'https://plataformaelectoral.jne.gob.pe/Candidato/GetCandidatos/{PARTIDO_POLITICO["idTipoEleccion"]}-{PARTIDO_POLITICO["idProcesoElectoral"]}-{PARTIDO_POLITICO["idSolicitudLista"]}-{PARTIDO_POLITICO["idExpediente"]}'
#     agent = {"User-Agent":"Mozilla/5.0"}

#     session = requests.Session()
#     retry = Retry(connect=3, backoff_factor=0.5)
#     adapter = HTTPAdapter(max_retries=retry)
#     session.mount('http://', adapter)
#     session.mount('https://', adapter)   
#     r = session.get(urlCandidatosPartidos, headers=agent)

#     responseJSON =r.json()
#     # Lista de candidatos por partido y region
#     listaDeCantidatos = responseJSON["data"]

#     if not len(listaDeCantidatos)> 0:
#       print("No hay lista de este partido")
#     else:
#       for CANDIDATO in listaDeCantidatos:
#         try:
#           # I need this to make other calls to others endpoints
#           CANDIDATO["idOrganizacionPolitica"] = PARTIDO_POLITICO["idOrganizacionPolitica"] 
          
#           listaDeCandidatos.append(CANDIDATO)
#           # print(CANDIDATO)
#           print(CANDIDATO["idOrganizacionPolitica"])
#         except:
#           print("No hay candidato")
#         counter = counter + 1
#         print(counter)

#   with open('GetCandidatos.json', 'w') as json_file:
#     json.dump(listaDeCandidatos, json_file)
  

# GetCandidatos()



with open(f'GetCandidatos.json', 'r', encoding='utf-8')as outFile:
  doc = outFile.read()
  arrayCandidatos = json.loads(str(doc))
  

def GetPersonalData():
  canditadosHVDatos = []
  counter = 0
  for CANDIDATO in arrayCandidatos:
      try:
        urlCandidatoListarPorID = f'https://plataformaelectoral.jne.gob.pe/HojaVida/GetAllHVDatosPersonales?param={CANDIDATO["idHojaVida"]}-0-{CANDIDATO["idOrganizacionPolitica"]}-{CANDIDATO["idProcesoElectoral"]}'
        r = requests.get(urlCandidatoListarPorID)
        data = r.json()
        candidatoHV = data["data"][0]
        counter = counter + 1
        print(counter)
        canditadosHVDatos.append(candidatoHV)

      except:
        print("CRASH OBTENIENDO LA HOJA DE VIDA")
    
  with open('CandidatoDatosPersonalesTest.json', 'w') as json_file:
    json.dump(canditadosHVDatos, json_file)


# GetPersonalData()


def GetExpLaboral():
  counter = 0
  resultDatosExpLaboral = []
  for CANDIDATO in arrayCandidatos:
    try:
      urlCandidatosExpLab = f'https://plataformaelectoral.jne.gob.pe/HojaVida/GetAllHVExpeLaboral?Ids={CANDIDATO["idHojaVida"]}-0-ASC'
      r = requests.get(urlCandidatosExpLab)

      data = r.json()
          # print(data)

      candidatoEduUni = data["data"]
      if not len(candidatoEduUni)> 0:
        print("No tiene exp este candidato")
      else:
        for candidatoEduUniSlot in candidatoEduUni:
          resultDatosExpLaboral.append(candidatoEduUniSlot)
      counter = counter + 1
      print(counter)

    except:
      print("CRASH OBTENIENDO LA HOJA DE VIDA")

  with open('CandidatoEduUnioral.json', 'w') as json_file:
    json.dump(resultDatosExpLaboral, json_file)

# GetExpLaboral()





def GetAllEduNOUni():
  resultEduNOUni = []
  counter = 0
  for CANDIDATO in arrayCandidatos:
    try:
      urlCandidatoListarPorID = f'https://plataformaelectoral.jne.gob.pe/HojaVida/GetAllHVNoUniversitaria?Ids={CANDIDATO["idHojaVida"]}-0'
      agent = {"User-Agent":"Mozilla/5.0"}

      r = requests.get(urlCandidatoListarPorID,headers=agent)
     
      data = r.json()
      candidatoEduNOUni = data["data"]
      if not len(candidatoEduNOUni)> 0:
        print("No tiene edu NO UNI este candidato")
      else:
        for candidatoEduNOUniSlot in candidatoEduNOUni:
          # adding key to indentifie the object
          candidatoEduNOUniSlot["strDocumentoIdentidad"] = CANDIDATO["strDocumentoIdentidad"]
          resultEduNOUni.append(candidatoEduNOUniSlot)
          print(candidatoEduNOUniSlot["strDocumentoIdentidad"])
      counter = counter + 1
      print(counter)
    except:
      print("CRASH OBTENIENDO LA EDUC NO  UNI")
    
  with open('CandidatoEduNOUni.json', 'w') as json_file:
    json.dump(resultEduNOUni, json_file)

# GetAllEduNOUni()




def GetAllEduUniversitaria():
  counter = 0
  resultDatosEduUni = []
  for CANDIDATO in arrayCandidatos:
    try:
      urlCandidatosEduUni = f'https://plataformaelectoral.jne.gob.pe/HojaVida/GetAllHVEduUniversitaria?Ids={CANDIDATO["idHojaVida"]}-0-ASC'
      agent = {"User-Agent":"Mozilla/5.0"}

      r = requests.get(urlCandidatosEduUni, headers=agent)
      data = r.json()

          # print(data)
      candidatoEduUni = data["data"]
      if not len(candidatoEduUni)> 0:
        print("No tiene exp este candidato")
      else:
        for candidatoEduUniSlot in candidatoEduUni:
          candidatoEduUniSlot["strDocumentoIdentidad"] = CANDIDATO["strDocumentoIdentidad"]
          resultDatosEduUni.append(candidatoEduUniSlot)
      counter = counter + 1
      print(counter)

    except:
      print("CRASH OBTENIENDO LA exp DE candidato")

  with open('CandidatoEduUni.json', 'w') as json_file:
    json.dump(resultDatosEduUni, json_file)


# GetAllEduUniversitaria()



def GetAllEduTecnico():
  resultEduTecnico = []
  counter = 0
  for CANDIDATO in arrayCandidatos:
    try:
      urlCandidatoListarPorID = f'https://plataformaelectoral.jne.gob.pe/HojaVida/GetAllHVEduTecnico?Ids={CANDIDATO["idHojaVida"]}-0'
      agent = {"User-Agent":"Mozilla/5.0"}

      r = requests.get(urlCandidatoListarPorID,headers=agent)
     
      data = r.json()
      candidatoEduTec = data["data"]
      if not len(candidatoEduTec)> 0:
        print("No tiene exp este candidato")
      else:
        for candidatoEduTecSlot in candidatoEduTec:
          # adding key to indentifie the object
          candidatoEduTecSlot["strDocumentoIdentidad"] = CANDIDATO["strDocumentoIdentidad"]
          resultEduTecnico.append(candidatoEduTecSlot)
          print(candidatoEduTecSlot["strDocumentoIdentidad"])
      counter = counter + 1
      print(counter)
    except:
      print("CRASH OBTENIENDO LA EDUC TECNICA")
    
  with open('CandidatoEduTecnica.json', 'w') as json_file:
    json.dump(resultEduTecnico, json_file)

# GetAllEduTecnico()





def GetAllPostGrado():
  resultSentPenales = []
  counter = 0
  for CANDIDATO in arrayCandidatos:
    try:
      urlCandidatoListarPorID = f'https://plataformaelectoral.jne.gob.pe/HojaVida/GetAllHVPosgrado?Ids={CANDIDATO["idHojaVida"]}-0'
      agent = {"User-Agent":"Mozilla/5.0"}

      r = requests.get(urlCandidatoListarPorID,headers=agent)
     
      data = r.json()
      candidatoSentPenal = data["data"]
      if not len(candidatoSentPenal)> 0:
        print("No tiene lista de educacion post grado este candidato")
      else:
        for candidatoSentPenalSlot in candidatoSentPenal:
          # adding key to indentifie the object
          candidatoSentPenalSlot["strDocumentoIdentidad"] = CANDIDATO["strDocumentoIdentidad"]
          resultSentPenales.append(candidatoSentPenalSlot)
          print(candidatoSentPenalSlot["strDocumentoIdentidad"])
      counter = counter + 1
      print(counter)
    except:
      print("CRASH OBTENIENDO LA EDUC post grado")
    
  with open('CandidatoSentPenal.json', 'w') as json_file:
    json.dump(resultSentPenales, json_file)

# GetAllPostGrado()





def GetAllSentenciaPenal():
  resultSentPenales = []
  counter = 0
  for CANDIDATO in arrayCandidatos:
    try:
      urlCandidatoListarPorID = f'https://plataformaelectoral.jne.gob.pe/HojaVida/GetAllHVSentenciaPenal?Ids={CANDIDATO["idHojaVida"]}-0-ASC'
      agent = {"User-Agent":"Mozilla/5.0"}

      r = requests.get(urlCandidatoListarPorID,headers=agent)
     
      data = r.json()
      candidatoSentPenal = data["data"]
      if not len(candidatoSentPenal)> 0:
        print("No tiene lista de sentencias penales , array 0")
      else:
        for candidatoSentPenalSlot in candidatoSentPenal:
          # adding key to indentifie the object
          candidatoSentPenalSlot["strDocumentoIdentidad"] = CANDIDATO["strDocumentoIdentidad"]
          resultSentPenales.append(candidatoSentPenalSlot)
          print(candidatoSentPenalSlot["strDocumentoIdentidad"])
      counter = counter + 1
      print(counter)
    except:
      print("CRASH OBTENIENDO LA sentencias penales ")
    
  with open('CandidatoSentPenal.json', 'w') as json_file:
    json.dump(resultSentPenales, json_file)

GetAllSentenciaPenal()