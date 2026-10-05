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

# CELL ********************

import requests
import pandas as pd

dato = "2026/10-05"   # format: ÅÅÅÅ/MM-DD
omrade = "NO1"        # NO1–NO5

url = f"https://www.hvakosterstrommen.no/api/v1/prices/{dato}_{omrade}.json"
r = requests.get(url, timeout=30)
r.raise_for_status()

df = pd.DataFrame(r.json())
print("Antall rader:", len(df))
print(df.columns.tolist())
display(df.head())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
