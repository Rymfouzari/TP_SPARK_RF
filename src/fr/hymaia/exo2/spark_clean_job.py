from pyspark.sql import SparkSession
from clean.data_cleaning import filtrer_major_clients, jointure_clients_city, add_departement_column
from pyspark.sql.functions import first

# Initialisation de la session Spark
spark = SparkSession.builder \
    .appName("SparkCleanJob") \
    .getOrCreate()

# Chargement des données
clients_df = spark.read.csv("src/resources/exo2/clients_bdd.csv", header=True, inferSchema=True)
villes_df = spark.read.csv("src/resources/exo2/city_zipcode.csv", header=True, inferSchema=True)

# Agrégation pour obtenir une ville unique par code postal
villes_unique = villes_df.groupBy("zip").agg(first("city").alias("city"))

# Pipeline de nettoyage : filtrage des majeurs, jointure avec ville et ajout de la colonne département
filtered_clients_major = filtrer_major_clients(clients_df)
joined_clients_city = filtered_clients_major.join(villes_unique, on="zip", how="left")
final_df = add_departement_column(joined_clients_city)

# Sauvegarde du DataFrame final nettoyé en Parquet
final_df.write.parquet("data/exo2/output/clean", mode="overwrite")
