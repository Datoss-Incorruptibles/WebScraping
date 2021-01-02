import json
import requests
from bs4 import BeautifulSoup
import csv

numeroDePaginas = 120
HOME_URL = 'https://www.webometrics.info/es/WORLD'

# XPATH_LINK_TO_ROW= '//div[@id="block-system-main"]/div/table[@class="sticky-enabled tableheader-processed sticky-table"]/tbody/tr'
arrayDeUnis = []
for i in range(numeroDePaginas):
  pass
  urlRanking = f'{HOME_URL}?page={i}'
  agent = {"User-Agent":"Mozilla/5.0"}
  r = requests.get(urlRanking, headers=agent)

  # B4S 
  # for link in soup.select("section.LINK_CLASS > div.LINK_CLASS2 > div.LINK_CLASS3 > a[href]"):
  #     print(link["href"])
  # .get_text()
  # .get('href')

  htmlRank = BeautifulSoup(r.text, 'lxml')
  listaDeUniv = htmlRank.select('div#block-system-main > div > table.sticky-enabled > tbody > tr')
  for universidad in listaDeUniv:
    uniInfo = universidad.select('td')
    rankingNum  = uniInfo[0].get_text() 
    universidadName = uniInfo[1].get_text() 
    # pais = uniInfo[0]
    presenciaPosicion = uniInfo[4].get_text() 
    impactoPosicion = uniInfo[5].get_text() 
    aperturaPosicion = uniInfo[6].get_text() 
    excelenciaPosicion = uniInfo[7].get_text() 

    objUni = {
      "rankingNum":rankingNum,
      "universidadName":universidadName,
      "presenciaPosicion":presenciaPosicion,
      "impactoPosicion":impactoPosicion,
      "aperturaPosicion":aperturaPosicion,
      "excelenciaPosicion":excelenciaPosicion
    }
    arrayDeUnis.append(objUni)


## generate csv 
# f = csv.writer(open("RankingUnis.csv", "w", newline=''))
# f.writerow(["rankingNum",
# "universidadName",
# "presenciaPosicion",
# "impactoPosicion", 
# "aperturaPosicion",
# "excelenciaPosicion"
# ])

# for uni in arrayDeUnis:
#   f.writerow([
#         uni["rankingNum"],
#         uni["universidadName"],
#         uni["presenciaPosicion"],
#         uni["impactoPosicion"],
#         uni["excelenciaPosicion"],
#         uni["excelenciaPosicion"]
#         ])

## insert db



con = connect_postgres()
print("me conecte?")
# for university in arrayDeUnis:


"""
INSERT INTO public.ranking_universidad(
	rankingnum, universidadname, presenciaposicion, impactoposicion, aperturaposicion, excelenciaposicion)
	VALUES (1,'Harvard University',1,2,1,1);

  """
