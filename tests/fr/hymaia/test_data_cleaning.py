from tests.fr.hymaia.spark_test_case import spark
import unittest
from src.fr.hymaia.exo2.clean.data_cleaning import filtrer_major_clients, jointure_clients_city, add_departement_column
from pyspark.sql import Row
from pyspark.sql.utils import AnalysisException

class TestDataCleaning(unittest.TestCase):

    def setUp(self):
        # Configuration d'un DataFrame de test pour les tests
        self.clients_df = spark.createDataFrame([
            Row(nom="Alice", age=25, zip="75001"),
            Row(nom="Bob", age=17, zip=None),
            Row(nom="Charlie", age=None, zip="75002"),
            Row(nom="David", age=19, zip="75003")
        ])

        self.villes_df = spark.createDataFrame([
            Row(zip="75001", ville="Paris"),
            Row(zip="75002", ville="Lyon"),
            # Pas de zip "75003" pour tester la jointure
        ])

    def test_filtrer_major_clients_nominal(self):
        # Vérifie que la fonction filtre correctement les clients majeurs
        input_df = spark.createDataFrame([
            Row(nom="Sophie", age=25),
            Row(nom="Lucas", age=15),
            Row(nom="Emma", age=30)
        ])
        expected_df = spark.createDataFrame([
            Row(nom="Sophie", age=25),
            Row(nom="Emma", age=30)
        ])
        
        actual_df = filtrer_major_clients(input_df)
        
        # Vérification des schémas
        expected_schema = expected_df.schema
        actual_schema = actual_df.schema
        
        self.assertEqual(actual_schema, expected_schema, "Les schémas des DataFrames ne correspondent pas.")
        
        # Vérification des valeurs
        self.assertEqual(actual_df.collect(), expected_df.collect())
    
    def test_jointure_clients_city(self):
        # Vérifie que la fonction joint correctement les clients et les villes
        clients_df = spark.createDataFrame([
            Row(nom="Sophie", zip="75001"),
            Row(nom="Lucas", zip="69001")
        ])
        villes_df = spark.createDataFrame([
            Row(zip="75001", ville="Paris"),
            Row(zip="69001", ville="Lyon")
        ])
        expected_df = spark.createDataFrame([
            Row(nom="Sophie", zip="75001", ville="Paris"),
            Row(nom="Lucas", zip="69001", ville="Lyon")
        ])
        
        actual_df = jointure_clients_city(clients_df, villes_df)

        # Réorganise le DataFrame pour s'assurer que les colonnes sont dans le bon ordre
        actual_df = actual_df.select("nom", "zip", "ville")
        
        # Vérification des schémas
        expected_schema = expected_df.schema
        actual_schema = actual_df.schema
        
        self.assertEqual(actual_schema, expected_schema, "Les schémas des DataFrames ne correspondent pas.")
        
        # Vérification des valeurs
        self.assertEqual(actual_df.collect(), expected_df.collect())

    def test_filtrer_major_clients_error(self):
        # Test d'erreur lorsque la colonne 'age' est absente
        input_df = spark.createDataFrame([Row(nom="Sophie"), Row(nom="Lucas")])
        with self.assertRaises(Exception):
            filtrer_major_clients(input_df)

    def test_add_departement_column_with_invalid_zip(self):
        # Test d'ajout de la colonne département avec des valeurs de zip invalides
        df_with_invalid_zip = spark.createDataFrame([
            Row(nom="Eve", age=30, zip="1234"),  # Zip trop court
            Row(nom="Frank", age=22, zip="20250"),  # Zip valide
            Row(nom="Grace", age=29, zip=None)  # Zip None
        ])

        result_df = add_departement_column(df_with_invalid_zip)

        # On s'attend à ce que les départements soient '01' pour le zip '1234'
        expected_departements = ["01", "2B", None]  # None pour le zip None
        actual_departements = result_df.select("departement").rdd.flatMap(lambda x: x).collect()

        # Vérification que les départements calculés sont corrects
        self.assertEqual(actual_departements, expected_departements)

# Exécution des tests
if __name__ == '__main__':
    unittest.main()
