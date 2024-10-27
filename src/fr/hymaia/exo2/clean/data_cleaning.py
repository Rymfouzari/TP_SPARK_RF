from pyspark.sql.functions import col, when, trim, length, lpad

# Filtrer les clients majeurs
def filtrer_major_clients(df):
    return df.filter(col("age") >= 18)

# Jointure avec la table de villes
def jointure_clients_city(clients, villes):
    return clients.join(villes, "zip")


# Ajout de la colonne département
def add_departement_column(df):
    return df.withColumn(
        "zip",
        when(length(trim(col("zip"))) == 4, lpad(trim(col("zip")), 5, '0')).otherwise(trim(col("zip")))
    ).withColumn(
        "departement",
        when((col("zip").cast("string") >= "20100") & (col("zip").cast("string") <= "20190"), "2A")
        .when((col("zip").cast("string") >= "20200") & (col("zip").cast("string") <= "20299"), "2B")
        .otherwise(col("zip").cast("string").substr(1, 2))
    )
