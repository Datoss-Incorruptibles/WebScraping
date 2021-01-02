import sys

sys.path.insert(1, './database')

import schedule
import time
import datetime
import shutil
from multiprocessing import Process
import insert_info as ii

def inserData():
  print("insert data start")
  if __name__ == '__main__':
    print("gg")
    p1 = Process(target=ii.insertExpedientesLista)

    # p2 = Process(target=ii.insertGetCandidatos)

    # p3 = Process(target=ii.insertInfoPersonal)
    # p4 = Process(target=ii.insertEduBasic)
    # p5 = Process(target=ii.insertEduNoUni)
    # p6 = Process(target=ii.insertEduTecnica)
    # p7 = Process(target=ii.insertEduUni)
    # p8 = Process(target=ii.insertExpLab)

    # p9 = Process(target=ii.insertSentCivil)
    # p10 = Process(target=ii.insertSentPenal)
    
    # p11 = Process(target=ii.insertCargoEleccion)
    # p12 = Process(target=ii.insertCargoPartidario)


    p1.start()
    # p2.start()
    # p3.start()
    # p4.start()
    # p5.start()
    # p6.start()
    # p7.start()
    # p8.start()
    # p9.start()
    # p10.start()
    # p11.start()
    # p12.start()


    p1.join()
    # p2.join()

    # p11.join()
    # p12.join()

  print("insert data end")

def moveData():
  now = datetime.datetime.now()
  cleanTime = now.strftime("%Y-%m-%d %H:%M:%S")
  # print ("Current date and time : ")
  # print (now.strftime("%Y-%m-%d %H:%M:%S"))
  # get current files and create and move to a timestamp folder
  shutil.move("../currentRawData/GetExpedientesLista.json", f'../dataHistory/GetExpedientesLista{cleanTime}_.json')
  # shutil.move("../currentRawData/GetCandidatos.json", f'../dataHistory/GetCandidatos{cleanTime}_.json')
  # shutil.move("../currentRawData/CandidatoDatosHV.json", f'../dataHistory/CandidatoDatosHV{cleanTime}_.json')
  print("save data end")

# def testData():
#   for i in range(1000):
#     print(i)

def job():
  print("Start job... inserts and save")
  inserData()
  # testData()
  # moveData()

job()

# # schedule.every(10).minutes.do(job)
# # schedule.every().day.at("02:30").do(job)
# schedule.every().minute.at(":50").do(job)

# while True:
#   schedule.run_pending()
#   time.sleep(1)