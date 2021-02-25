import json
import requests
from bs4 import BeautifulSoup
import csv
import datetime
import time
import re

today = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def scrapRanking():
      # generate csv
  f = csv.writer(open("rankingUnis.csv", "w", newline=''))
  f.writerow(["rankingNum",
  "universidadName",
  "paisName",
  "urlImagePais",
  "impactoPosicion",
  "aperturaPosicion",
  "excelenciaPosicion",
  "fecha_registro"
  ])


  numeroDePaginas = 120
  HOME_URL = 'https://www.webometrics.info/es/WORLD'
  counter = 0
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
      urlImagePais = uniInfo[3].center.img['src']
      x = re.search("[^/]+(?=/$|$)", urlImagePais)
      paisPng = x.group()
      paisName = paisPng.replace(".png", "")
      impactoPosicion = uniInfo[4].get_text()
      aperturaPosicion = uniInfo[5].get_text()
      excelenciaPosicion = uniInfo[6].get_text()

      # excelenciaPosicion = uniInfo[7].get_text()
      f.writerow([
            rankingNum,
            universidadName,
            paisName,
            urlImagePais,
            impactoPosicion,
            aperturaPosicion,
            excelenciaPosicion,
            today
            ])

      counter= counter + 1
      print(counter)


scrapRanking()
