import json
import requests
from bs4 import BeautifulSoup
import csv
import datetime
import time

import psycopg2
import os
from os.path import join, dirname
from dotenv import load_dotenv
load_dotenv('../../.env')
today = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

con = psycopg2.connect(database=os.getenv("DB_NAME"), user=os.getenv("DB_USER"), password=os.getenv("DB_PASS"), host=os.getenv("DB_HOST"), port="5432")
print(con)
print("Database opened successfully")

def scrapRanking():
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
      # pais = uniInfo[0]
      impactoPosicion = uniInfo[4].get_text()
      aperturaPosicion = uniInfo[5].get_text()
      excelenciaPosicion = uniInfo[6].get_text()

      # excelenciaPosicion = uniInfo[7].get_text()
      objUni = {
        "rankingNum":rankingNum,
        "universidadName":universidadName,
        "impactoPosicion":impactoPosicion,
        "aperturaPosicion":aperturaPosicion,
        "excelenciaPosicion":excelenciaPosicion
      }
      arrayDeUnis.append(objUni)
      counter= counter + 1
      print(counter)

  print("F")



  ## generate csv
  f = csv.writer(open("rankingUnis.csv", "w", newline=''))
  f.writerow(["rankingNum",
  "universidadName",
  "impactoPosicion",
  "aperturaPosicion",
  "excelenciaPosicion",
  "fecha_registro"
  ])

  for uni in arrayDeUnis:
    f.writerow([
          uni["rankingNum"],
          uni["universidadName"],
          uni["impactoPosicion"],
          uni["aperturaPosicion"],
          uni["excelenciaPosicion"],
          today
          ])
  print("FF")



## insert db



def createRankingUnis():
    cur = con.cursor()
    cur.execute('''
    drop table IF EXISTS de.ranking_unis;

    CREATE TABLE IF NOT EXISTS de.ranking_unis(
        rankingNum character varying,
        universidadName character varying,
        impactoPosicion character varying,
        aperturaPosicion character varying,
        excelenciaPosicion character varying,
        fecha_registro TIMESTAMP DEFAULT now() NOT NULL
        );
    ''')
    con.commit()
    print("Table ranking_unis created successfully")



def uploadRanking():
  cur = con.cursor()
  cur.execute("TRUNCATE de.ranking_unis;")
  copy_sql = """
            COPY de.ranking_unis FROM stdin WITH CSV HEADER
            DELIMITER as ','
            """

  with open('./rankingUnis.csv', 'r') as f:
    cur.copy_expert(sql=copy_sql, file=f)
    con.commit()
    print("Table rankingUnis cargado successfully")



# scrapRanking

# createRankingUnis()

# uploadRanking()
