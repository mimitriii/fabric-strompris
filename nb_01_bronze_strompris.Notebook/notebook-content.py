# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "bf8bcd60-5823-465a-95b1-686ab37040d8",
# META       "default_lakehouse_name": "lh_strompris",
# META       "default_lakehouse_workspace_id": "1ab56f10-6650-48e8-8f0f-1b8312f267d8",
# META       "known_lakehouses": [
# META         {
# META           "id": "bf8bcd60-5823-465a-95b1-686ab37040d8"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

start_dato = "2025-10-01"   # første dag som skal hentes
slutt_dato = ""             # tom = i morgen (morgendagens priser publiseres ca. kl. 13)
overskriv = False           # True = hent på nytt selv om filen finnes

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests, time
from datetime import date, timedelta

OMRADER = ["NO1", "NO2", "NO3", "NO4", "NO5"]
BASE = "https://www.hvakosterstrommen.no/api/v1/prices"
ROOT = "Files/bronze/strompris"

start = date.fromisoformat(start_dato)
slutt = date.fromisoformat(slutt_dato) if slutt_dato else date.today() + timedelta(days=1)

session = requests.Session()
hentet = hoppet = mangler = 0

d = start
while d <= slutt:
    for omr in OMRADER:
        sti = f"{ROOT}/{omr}/{d:%Y}/{d:%m-%d}.json"
        if not overskriv and notebookutils.fs.exists(sti):
            hoppet += 1
            continue

        url = f"{BASE}/{d:%Y}/{d:%m-%d}_{omr}.json"
        r = session.get(url, timeout=30)
        if r.status_code == 404:      # f.eks. morgendagens priser er ikke publisert ennå
            mangler += 1
            continue
        r.raise_for_status()

        notebookutils.fs.put(sti, r.text, True)
        hentet += 1
        time.sleep(0.05)              # vær snill mot API-et
    d += timedelta(days=1)

print(f"Hentet: {hentet} | Hoppet over (fantes): {hoppet} | Ikke tilgjengelig: {mangler}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

df = (spark.read.option("multiline", True)
      .json(f"{ROOT}/*/*/*.json")
      .withColumn("kilde_fil", F.input_file_name()))

print("Antall rader totalt:", df.count())
display(df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
