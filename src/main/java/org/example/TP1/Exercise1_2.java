package org.example.TP1;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;
public class Exercise1_2 {

    public static void main(String[] args) {
        // Créez une session Spark
        SparkSession spark = SparkSession.builder()
                .appName("TopYears")
                .master("local[*]")  // le mode de fonctionnement de Spark (local dans cet exemple)
                .getOrCreate();

        // Chargez le fichier CSV en tant que DataFrame
        Dataset<Row> incidentsDF = spark.read()
                .option("header", "true")
                .option("multiline",true)
                .csv("C:/Work/Big Data/untitled/src/main/resources/incidents.csv");

        // Ajoutez une colonne "Year" en extrayant l'année de la colonne "date"
        incidentsDF = incidentsDF.withColumn("Year", year(col("date")));

        // Effectuez une agrégation par année pour compter le nombre total d'incidents par année
        Dataset<Row> incidentsParAnnee = incidentsDF.groupBy("Year").agg(count("Id").alias("NombreIncidents"));

        // Triez les résultats par ordre décroissant pour obtenir les années avec le plus grand nombre d'incidents
        incidentsParAnnee = incidentsParAnnee.orderBy(col("NombreIncidents").desc());

        // Affichez les deux années avec le plus grand nombre d'incidents
        incidentsParAnnee.show(); //exe3

        // Arrêtez la session Spark
        //spark.stop();
    }
}
