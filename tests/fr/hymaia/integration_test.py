# tests/fr/hymaia/integration_tests.py

import unittest
from tests.fr.hymaia.spark_test_case import spark
from pyspark.sql import Row
from src.fr.hymaia.exo2.clean.data_cleaning import add_departement_column  # Import de la fonction de nettoyage
from src.fr.hymaia.exo2.aggregate.data_aggregation import calculate_population_by_departement  # Import de la fonction d'agrégation

class TestSparkJobs(unittest.TestCase):

    def test_spark_clean_job(self):
        # Test de la fonction add_departement_column
        df = spark.createDataFrame([
            Row(nom="Alice", age=25, zip="75001"),
            Row(nom="Bob", age=17, zip=None),
            Row(nom="Charlie", age=None, zip="75002"),
        ])

        result_df = add_departement_column(df)

        # On s'attend à ce que les départements soient "75" pour 75001 et "75" pour 75002
        expected_departements = ["75", None, "75"]
        actual_departements = result_df.select("departement").rdd.flatMap(lambda x: x).collect()

        # Vérification que les départements calculés sont corrects
        self.assertEqual(actual_departements, expected_departements)

    def test_spark_aggregate_job(self):
        # Test de la fonction calculate_population_by_departement
        df = spark.createDataFrame([
            Row(nom="Alice", departement="75"),
            Row(nom="Bob", departement="75"),
            Row(nom="Charlie", departement="69"),
        ])

        result_df = calculate_population_by_departement(df)

        expected_data = [Row(departement="75", nb_people=2), Row(departement="69", nb_people=1)]
        actual_data = result_df.collect()

        # Vérification que les données agrégées sont correctes
        self.assertEqual(actual_data, expected_data)

if __name__ == "__main__":
    unittest.main()