import sys
sys.path.insert(1, './database')

import schedule
import scraper
import time
from database import multiproc

def job():
    print("Job start .... scraper")
    scraper.GetExpedientesLista()
    # scraper.GetCandidatos()
    # GetPersonalData()
    print("Job end .... scraper")

    print("Start job... inserts and save")
    multiproc.inserData()
    # multiproc.moveData()
    print("End job... inserts and save")

# schedule.every(10).minutes.do(job)
# schedule.every().day.at("02:30").do(job)
# schedule.every(1).minute.at(":10").do(job)

job()

# while True:
#     schedule.run_pending()
#     time.sleep(1)






