from tests.fr.hymaia.spark_test_case import spark
import unittest
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, LongType
from src.fr.hymaia.exo2.aggregate.data_aggregation import calculate_population_by_departement

class TestPopulationCalculation(unittest.TestCase):

    def test_calculate_population_by_departement(self):
        # Création d'un DataFrame d'entrée
        input_df = spark.createDataFrame([
            Row(nom="Alice", departement="01"),
            Row(nom="Bob", departement="02"),
            Row(nom="Charlie", departement="01"),
            Row(nom="David", departement="03"),
            Row(nom="Eve", departement="02"),
            Row(nom="Frank", departement="01"),
            Row(nom="Grace", departement="03"),
        ])
        
        # Définition explicite du schéma pour le DataFrame attendu
        expected_schema = StructType([
            StructField("departement", StringType(), True),
            StructField("nb_people", LongType(), False)  # Définir comme non nullable
        ])
        
        # DataFrame attendu avec schéma explicitement défini
        expected_df = spark.createDataFrame([
            Row(departement="01", nb_people=3),
            Row(departement="02", nb_people=2),
            Row(departement="03", nb_people=2)
        ], schema=expected_schema)  # Utiliser le schéma ici

        # Appel de la fonction à tester
        actual_df = calculate_population_by_departement(input_df)

        # Réorganise le DataFrame pour s'assurer que les colonnes sont dans le bon ordre
        actual_df = actual_df.select("departement", "nb_people")

        # Vérification des schémas
        expected_schema = expected_df.schema
        actual_schema = actual_df.schema
        
        # Vérification des schémas - ajusté pour la nullabilité
        self.assertEqual(actual_schema, expected_schema, "Les schémas des DataFrames ne correspondent pas.")

        # Vérification des valeurs
        self.assertEqual(actual_df.collect(), expected_df.collect())

# Exécution des tests
if __name__ == '__main__':
    unittest.main()
