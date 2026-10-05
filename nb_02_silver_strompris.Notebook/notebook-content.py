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

from pyspark.sql import functions as F

spark.conf.set("spark.sql.session.timeZone", "UTC")
ROOT = "Files/bronze/strompris"

# Bruk skjemaet "silver" hvis Lakehouse-et har skjemaer, ellers et prefiks
try:
    spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
    MAL = "silver.strompris_time"
except Exception:
    MAL = "silver_strompris_time"
print("Skriver til:", MAL)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

raw = (spark.read.option("multiline", True)
       .json(f"{ROOT}/*/*/*.json")
       .withColumn("kilde_fil", F.input_file_name()))

silver = (raw
    .withColumn("prisomrade", F.regexp_extract("kilde_fil", r"/(NO[1-5])/", 1))
    .withColumn("tid_start_utc", F.to_timestamp("time_start"))
    .withColumn("tid_slutt_utc", F.to_timestamp("time_end"))
    .withColumn("tid_start_lokal", F.from_utc_timestamp("tid_start_utc", "Europe/Oslo"))
    .withColumn("dato", F.to_date("tid_start_lokal"))
    .withColumn("time", F.hour("tid_start_lokal"))
    .select(
        "prisomrade", "dato", "time",
        "tid_start_lokal", "tid_start_utc", "tid_slutt_utc",
        F.col("NOK_per_kWh").cast("decimal(10,5)").alias("nok_per_kwh"),
        F.col("EUR_per_kWh").cast("decimal(10,5)").alias("eur_per_kwh"),
        F.col("EXR").cast("decimal(10,4)").alias("eur_nok_kurs"),
        "kilde_fil")
    .withColumn("lastet_tid", F.current_timestamp())
    .dropDuplicates(["prisomrade", "tid_start_utc"]))

display(silver.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Rader:", silver.count())
print("Mangler prisområde:", silver.filter("prisomrade = ''").count())
print("Mangler pris:", silver.filter("nok_per_kwh IS NULL").count())
print("Timer med negativ pris:", silver.filter("nok_per_kwh < 0").count())

# Dager som ikke har 24 timer (forventer kun sommertidsdagene)
avvik = silver.groupBy("prisomrade", "dato").count().filter("count != 24")
display(avvik.orderBy("dato", "prisomrade"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(silver.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .format("delta")
    .saveAsTable(MAL))

print("Ferdig. Rader i tabellen:", spark.table(MAL).count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
