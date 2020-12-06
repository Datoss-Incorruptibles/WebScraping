import requests
from bs4 import BeautifulSoup
import csv
import json

PARTIDOSPOLITICOS = []
arrayCandidatosEducacionBasicaRegular = []

with open(f'PresidenteCongresoParlamentoAndino.json', 'r', encoding='utf-8')as outFile:
    doc = outFile.read()
    docString = json.loads(str(doc))

    for partidoPolitico in docString:
        TXORGPOLITICA = partidoPolitico["TXORGPOLITICA"]
        IDEXPEDIENTE = partidoPolitico["IDEXPEDIENTE"]
        OBJPARTIDO = {"TXORGPOLITICA":TXORGPOLITICA, "IDEXPEDIENTE":IDEXPEDIENTE}
        PARTIDOSPOLITICOS.append(OBJPARTIDO)
# print(PARTIDOSPOLITICOS)


for PARTIDO_POLITICO in PARTIDOSPOLITICOS:
  urlCandidatosPartidos = f'https://consultalistacandidato.jne.gob.pe/PresidenteCongresoParlamentoAndino/GetCandidatos?IDPROCESO=79&IDEXPEDIENTE={PARTIDO_POLITICO["IDEXPEDIENTE"]}'
  agent = {"User-Agent":"Mozilla/5.0"}
  r = requests.get(urlCandidatosPartidos, headers=agent)
  htmlCantidatos = BeautifulSoup(r.text, 'lxml')

  listaDeCantidatos = htmlCantidatos.find('table', attrs={'class':'Candidato'}).find_all('tr')

  TRESCANDIDATOS = []
  for i in range(3): 
      # print(cantidatoOne)
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
      TRESCANDIDATOS.append(CANDIDATOABC)

  PARTIDO_POLITICO["CANDIDATOS"] = TRESCANDIDATOS

  for CANDIDATO in PARTIDO_POLITICO["CANDIDATOS"]:
    if CANDIDATO["HOJA_VIDA_ENCRIPTADA"] == "VACIO":
      print("NO HAY HOJA DE VIDA DE ESTE CANDIDATO")

    else:

      urlEducacionBasicaListarPorCandidato = 'https://pecaoe.jne.gob.pe/servicios/declaracion.asmx/EducacionBasicaListarPorCandidato'
      myBody = {"objCandidatoBE": {"intId_Candidato": CANDIDATO["IDCANDIDATO"], "objProcesoElectoralBE": {"intIdProceso": CANDIDATO["IDPROCESO"]}}}

      r = requests.post(urlEducacionBasicaListarPorCandidato, json=myBody)
      # print(r)
      # print(r.status_code)
      # print(r.headers['content-type'])
      # print(r.json())
      responseJSON =r.json() 

      len(responseJSON["d"])
      arrayEducacionBasicaRegular = []

      for CandEduBasRegular in responseJSON["d"]:
          objCandidatoEducacionBasicaRegular = {
              "IDCANDIDATO":CANDIDATO["IDCANDIDATO"],
              "DNI":CANDIDATO["DNI"],
              "NOMBRE_COMPLETO":CANDIDATO["NOMBRE_COMPLETO"],
              "intTipoEducacion": CandEduBasRegular["intTipoEducacion"], # 1:PRIMARIA  #2:SECUNDARIA
              "strCentroPrimaria": CandEduBasRegular["strCentroPrimaria"],
              "strPrimaria":CandEduBasRegular["strPrimaria"],
              "objUbigeoPrimaria_strDepartamento":CandEduBasRegular["objUbigeoPrimaria"]["strDepartamento"],
              "objUbigeoPrimaria_strProvincia":CandEduBasRegular["objUbigeoPrimaria"]["strProvincia"], 
              "objUbigeoPrimaria_strDistrito":CandEduBasRegular["objUbigeoPrimaria"]["strDistrito"], 
              "intAnioInicioPrimaria":CandEduBasRegular["intAnioInicioPrimaria"],
              "intAnioFinPrimaria":CandEduBasRegular["intAnioFinPrimaria"],
              "strCentroSecundaria":CandEduBasRegular["strCentroSecundaria"],
              "strSecundaria":CandEduBasRegular["strSecundaria"],
              "objUbigeoSecundaria_strDepartamento":CandEduBasRegular["objUbigeoSecundaria"]["strDepartamento"],
              "objUbigeoSecundaria_strProvincia":CandEduBasRegular["objUbigeoSecundaria"]["strProvincia"],
              "objUbigeoSecundaria_strDistrito":CandEduBasRegular["objUbigeoSecundaria"]["strDistrito"],
              "intAnioInicioSecundaria":CandEduBasRegular["intAnioInicioSecundaria"],
              "intAnioFinSecundaria":CandEduBasRegular["intAnioFinSecundaria"],
              "strPais":CandEduBasRegular["strPais"],
              "strGradoI":CandEduBasRegular["strGradoI"],
              "strGradoII":CandEduBasRegular["strGradoII"],
              "strGradoIII":CandEduBasRegular["strGradoIII"],
              "strGradoIV":CandEduBasRegular["strGradoIV"],
              "strGradoV":CandEduBasRegular["strGradoV"],
              "strGradoVI":CandEduBasRegular["strGradoVI"]
          }
          arrayEducacionBasicaRegular.append(objCandidatoEducacionBasicaRegular)

      arrayCandidatosEducacionBasicaRegular.append(arrayEducacionBasicaRegular)   
          
print(arrayCandidatosEducacionBasicaRegular)
    
    
    


f = csv.writer(open("EducacionBasica.csv", "w", newline=''))

f.writerow(["IDCANDIDATO",
"DNI",
"NOMBRE_COMPLETO",
"intTipoEducacion", 
"strCentroPrimaria",
"strPrimaria",
"objUbigeoPrimaria_strDepartamento",
"objUbigeoPrimaria_strProvincia",
"objUbigeoPrimaria_strDistrito",
"intAnioInicioPrimaria",
"intAnioFinPrimaria",
"strCentroSecundaria",
"strSecundaria",
"objUbigeoSecundaria_strDepartamento",
"objUbigeoSecundaria_strProvincia",
"objUbigeoSecundaria_strDistrito",
"intAnioInicioSecundaria",
"intAnioFinSecundaria",
"strPais",
"strGradoI",
"strGradoII",
"strGradoIII",
"strGradoIV",
"strGradoV",
"strGradoVI"
])

for candidatoEduBasicaReg in arrayCandidatosEducacionBasicaRegular:
  for candidatoEduBasica in candidatoEduBasicaReg:
    f.writerow([
          candidatoEduBasica["IDCANDIDATO"],
          candidatoEduBasica["DNI"],
          candidatoEduBasica["NOMBRE_COMPLETO"],
          candidatoEduBasica["intTipoEducacion"],
          candidatoEduBasica["strCentroPrimaria"],
          candidatoEduBasica["strPrimaria"],
          candidatoEduBasica["objUbigeoPrimaria_strDepartamento"],
          candidatoEduBasica["objUbigeoPrimaria_strProvincia"],
          candidatoEduBasica["objUbigeoPrimaria_strDistrito"],
          candidatoEduBasica["intAnioInicioPrimaria"],
          candidatoEduBasica["intAnioFinPrimaria"],
          candidatoEduBasica["strCentroSecundaria"],
          candidatoEduBasica["strSecundaria"],
          candidatoEduBasica["objUbigeoSecundaria_strDepartamento"],
          candidatoEduBasica["objUbigeoSecundaria_strProvincia"],
          candidatoEduBasica["objUbigeoSecundaria_strDistrito"],
          candidatoEduBasica["intAnioInicioSecundaria"],
          candidatoEduBasica["intAnioFinSecundaria"],
          candidatoEduBasica["strPais"],
          candidatoEduBasica["strGradoI"],
          candidatoEduBasica["strGradoII"],
          candidatoEduBasica["strGradoIII"],
          candidatoEduBasica["strGradoIV"],
          candidatoEduBasica["strGradoV"],
          candidatoEduBasica["strGradoVI"]
          ])

