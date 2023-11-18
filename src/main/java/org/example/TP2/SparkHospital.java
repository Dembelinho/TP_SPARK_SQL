package org.example.TP2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkHospital {
    public static void main(String[] args) {
        // Configuration de Spark
        SparkSession ss=SparkSession.builder().appName("DB_HOSPITAL SQL Spark").master("local[*]").getOrCreate();
        // Configuration de la base de données MySQL
        Dataset<Row> consultationsDF = ss.read().format("jdbc")
                .option("driver","com.mysql.cj.jdbc.Driver")
                .option("url","jdbc:mysql://localhost:3306/DB_HOPITAL")
                .option("query","SELECT * FROM `CONSULTATIONS` WHERE 1") // Charger les données de la table CONSULTATIONS depuis MySQL
                .option("user","root")
                .option("password","")
                .load();
        // pour afficher le nombre de consultations par jour.
        consultationsDF.groupBy("DATE_CONSULTATION").count().show(); //exe 4



        Dataset<Row> medecinsDF = ss.read().format("jdbc")
                .option("driver","com.mysql.cj.jdbc.Driver")
                .option("url","jdbc:mysql://localhost:3306/DB_HOPITAL")
                .option("query","SELECT * FROM `MEDECINS` WHERE 1") // Charger les données de la table MEDECINS depuis MySQL
                .option("user","root")
                .option("password","")
                .load();
        // faire la jointure entre la table CONSULTATIONS et la table MEDECINS
        Dataset<Row> jDF = consultationsDF.join(medecinsDF, consultationsDF.col("ID_MEDECIN")
                .equalTo(medecinsDF.col("ID")));

        // pour obtenir le nombre de consultations par médecin
        Dataset<Row> consulCountByMed = jDF.groupBy("NOM", "PRENOM").count()
                .withColumnRenamed("count", "NB_CONSULTATION");

        // Afficher les résultats
        consulCountByMed.show(); //exe5


        Dataset<Row> patientsDF = ss.read().format("jdbc")
                .option("driver","com.mysql.cj.jdbc.Driver")
                .option("url","jdbc:mysql://localhost:3306/DB_HOPITAL")
                .option("query","SELECT * FROM `PATIENTS` WHERE 1") // Charger les données de la table PATIENTS depuis MySQL
                .option("user","root")
                .option("password","")
                .load();
        // pour obtenir le nombre de patients par médecin
        Dataset<Row> patCountByMed = jDF.groupBy("NOM", "PRENOM")
                .agg(org.apache.spark.sql.functions.countDistinct("ID_PATIENT").alias("NB_PATIENTS"));

        // Afficher les résultats
        patCountByMed.show(); //exe6
    }

}
