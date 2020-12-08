import requests
from bs4 import BeautifulSoup
    
import csv
import json

PARTIDOSPOLITICOS = []
arrayCandidatosExperiencia = []

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
      TRESCANDIDATOS.append(CANDIDATOABC)

  PARTIDO_POLITICO["CANDIDATOS"] = TRESCANDIDATOS

  for CANDIDATO in PARTIDO_POLITICO["CANDIDATOS"]:
    if CANDIDATO["HOJA_VIDA_ENCRIPTADA"] == "VACIO":
      print("NO HAY HOJA DE VIDA DE ESTE CANDIDATO")

    else:
      # url = f'https://pecaoe.jne.gob.pe/sipe/HojaVidaEG.aspx?cod={CANDIDATO["HOJA_VIDA_ENCRIPTADA"]}'
   

      urlCandidatoExperienciaListarPorCandidato = 'https://pecaoe.jne.gob.pe/servicios/declaracion.asmx/CandidatoExperienciaListarPorCandidato'
      myBody = {"objCandidatoBE": {"intId_Candidato": CANDIDATO["IDCANDIDATO"], "objProcesoElectoralBE": {"intIdProceso": CANDIDATO["IDPROCESO"]}}}
          
          
      r = requests.post(urlCandidatoExperienciaListarPorCandidato, json=myBody)
      # print(r)
      # print(r.status_code)
      # print(r.headers['content-type'])
      # print(r.json())
      responseJSON =r.json() 

      len(responseJSON["d"])
      arrayCandidatoExperiencia = []

      for CandidatoExperiencia in responseJSON["d"]:
          objCandidatoExperiencia = {
              "IDCANDIDATO":CANDIDATO["IDCANDIDATO"],
              "DNI":CANDIDATO["DNI"],
              "NOMBRE_COMPLETO":CANDIDATO["NOMBRE_COMPLETO"],
              "intCondicion":CandidatoExperiencia["intCondicion"], # 1 condicional # 2 dependiente
              "strEmpleador":CandidatoExperiencia["strEmpleador"],
              "strRuc":CandidatoExperiencia["strRuc"],
              "strPais":CandidatoExperiencia["strPais"],
              "strNombre_Sector":CandidatoExperiencia["objTipoSectorBE"]["strNombre_Sector"],
              "strCargo":CandidatoExperiencia["strCargo"],
              "intInicioAnio":CandidatoExperiencia["intInicioAnio"],
              "intFinAnio":CandidatoExperiencia["intFinAnio"]}
          arrayCandidatoExperiencia.append(objCandidatoExperiencia)

      arrayCandidatosExperiencia.append(arrayCandidatoExperiencia)   
          
print(arrayCandidatosExperiencia)
    
    
    


# f = csv.writer(open("test.csv", "wb+"))
f = csv.writer(open("ExperienciaLaboral.csv", "w", newline=''))
# Write CSV Header, If you dont need that, remove this line
f.writerow(["IDCANDIDATO",
"DNI",
"NOMBRE_COMPLETO",
"intCondicion", 
"strEmpleador",
"strRuc",
"strPais",
"strNombre_Sector",
"strCargo",
"intInicioAnio",
"intFinAnio"
])

for candidatoExps in arrayCandidatosExperiencia:
  for candidatoExp in candidatoExps:
    f.writerow([
          candidatoExp["IDCANDIDATO"],
          candidatoExp["DNI"],
          candidatoExp["NOMBRE_COMPLETO"],
          candidatoExp["intCondicion"],
          candidatoExp["strEmpleador"],
          candidatoExp["strRuc"],
          candidatoExp["strPais"],
          candidatoExp["strNombre_Sector"],
          candidatoExp["strCargo"],
          candidatoExp["intInicioAnio"],
          candidatoExp["intFinAnio"]])
