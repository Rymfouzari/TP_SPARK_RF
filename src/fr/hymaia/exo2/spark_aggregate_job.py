from pyspark.sql import SparkSession
from aggregate.data_aggregation import calculate_population_by_departement

# Initialisation de la session Spark
spark = SparkSession.builder \
    .appName("SparkAggregateJob") \
    .getOrCreate()

# Chargement du DataFrame nettoyé
clean_df = spark.read.parquet("data/exo2/output/clean")

# Calcul de la population par département
population_df = calculate_population_by_departement(clean_df)

# Sauvegarde du résultat d'agrégation en CSV
population_df.coalesce(1).write.csv("data/exo2/output/aggregate", header=True, mode="overwrite")

population_df.show(1000)