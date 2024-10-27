from pyspark.sql.functions import col, count

# Calcul de la population par département
def calculate_population_by_departement(df):
    return df.groupBy("departement") \
             .agg(count("*").alias("nb_people")) \
             .orderBy(col("nb_people").desc(), col("departement").asc())
