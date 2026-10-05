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
from datetime import date

spark.sql("CREATE SCHEMA IF NOT EXISTS gold")
s = spark.table("silver.strompris_time")

def lagre(df, navn):
    (df.write.mode("overwrite").option("overwriteSchema", "true")
       .format("delta").saveAsTable(f"gold.{navn}"))
    print(f"gold.{navn}: {spark.table('gold.' + navn).count()} rader")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

omrader = [
    ("NO1", "Østlandet", 1),
    ("NO2", "Sørvestlandet", 2),
    ("NO3", "Midt-Norge", 3),
    ("NO4", "Nord-Norge", 4),
    ("NO5", "Vestlandet", 5),
]
dim_prisomrade = spark.createDataFrame(omrader, "prisomrade string, omradenavn string, sortering int")
lagre(dim_prisomrade, "dim_prisomrade")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

g = s.agg(F.min("dato").alias("fra"), F.max("dato").alias("til")).first()
fra = date(g.fra.year, 1, 1)      # hele år, slik Power BI liker datotabeller
til = date(g.til.year, 12, 31)

maaneder = ["januar", "februar", "mars", "april", "mai", "juni",
            "juli", "august", "september", "oktober", "november", "desember"]
ukedager = ["mandag", "tirsdag", "onsdag", "torsdag", "fredag", "lørdag", "søndag"]

dim_dato = (spark.sql(f"SELECT explode(sequence(DATE'{fra}', DATE'{til}', INTERVAL 1 DAY)) AS dato")
    .withColumn("aar", F.year("dato"))
    .withColumn("kvartal", F.concat(F.lit("K"), F.quarter("dato")))
    .withColumn("maaned_nr", F.month("dato"))
    .withColumn("maaned", F.element_at(F.array(*[F.lit(m) for m in maaneder]), F.month("dato")))
    .withColumn("aar_maaned", F.date_format("dato", "yyyy-MM"))
    .withColumn("uke", F.weekofyear("dato"))
    .withColumn("ukedag_nr", ((F.dayofweek("dato") + 5) % 7) + 1)   # mandag = 1
    .withColumn("ukedag", F.element_at(F.array(*[F.lit(d) for d in ukedager]), F.col("ukedag_nr")))
    .withColumn("er_helg", F.col("ukedag_nr") >= 6))

lagre(dim_dato, "dim_dato")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

t = F.col("time")
dim_time = (spark.range(24).select(F.col("id").cast("int").alias("time"))
    .withColumn("time_tekst", F.format_string("%02d:00", t))
    .withColumn("tidsrom", F.when(t < 6, "Natt (00–06)")
                            .when(t < 12, "Morgen (06–12)")
                            .when(t < 18, "Dag (12–18)")
                            .otherwise("Kveld (18–24)"))
    .withColumn("tidsrom_sort", (t / 6).cast("int") + 1))

lagre(dim_time, "dim_time")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fakt = (s.select("prisomrade", "dato", "time", "tid_start_lokal", "tid_start_utc",
                 "nok_per_kwh", "eur_per_kwh", "eur_nok_kurs")
         .withColumn("ore_per_kwh", (F.col("nok_per_kwh") * 100).cast("decimal(10,3)")))

lagre(fakt, "fakt_strompris_time")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
