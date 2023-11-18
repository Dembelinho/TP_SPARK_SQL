# TP_SPARK_SQL

![MySQL](https://img.shields.io/badge/mysql-%2300f.svg?style=for-the-badge&logo=mysql&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-FDEE21?style=flat-square&logo=apachespark&logoColor=black)
![Apache Hadoop](https://img.shields.io/badge/Apache%20Hadoop-66CCFF?style=for-the-badge&logo=apachehadoop&logoColor=black)
![IntelliJ IDEA](https://img.shields.io/badge/IntelliJIDEA-000000.svg?style=for-the-badge&logo=intellij-idea&logoColor=white)
![Java](https://img.shields.io/badge/java-%23ED8B00.svg?style=for-the-badge&logo=openjdk&logoColor=white)

## SparkSQL
![saprk sql](https://github.com/Dembelinho/TP_SPARK_SQL/assets/110602716/f3a4edb0-1c7b-4720-86ba-179469a48c34)

Spark SQL est un module Spark conçu pour le traitement de données structurées. Il apporte une couche d'abstraction programmatique appelée DataFrames et peut également faire office de moteur de requêtes SQL distribué. 
Il permet d'exécuter les requêtes Hadoop Hive 100 fois plus vite sur les déploiements et données existants, sans modification. 

Spark SQL apporte une prise en charge native de SQL à Spark et uniformise le processus d'interrogation des données stockées à la fois dans les RDD (les datasets distribués de Spark) et dans des sources externes. *
Spark SQL possède l'avantage pratique d'estomper la frontière entre les RDD et les tables relationnelles. 

Grâce à l'unification de ces deux abstractions puissantes, les développeurs peuvent désormais utiliser des commandes SQL pour interroger des données externes et procéder à des analyses complexes, au sein d'une même application. 
Concrètement, Spark SQL permet aux développeurs de :
- Importer des données relationnelles depuis des fichiers Parquet et des tables Hive
- Exécuter des requêtes SQL sur des données importées et des RDD existants
- Rédiger facilement des RDD à partir de tables Hive ou de fichiers Parquet
  
Spark SQL comprend également un optimiseur de coût, un dispositif de stockage en colonne et un outil de génération de code pour accélérer la création de requêtes.
Dans le même temps, il est capable de prendre en compte des milliers de nœuds et des requêtes de plusieurs heures grâce au moteur Spark, qui assure une tolérance totale aux défaillances en cours de requête.
Autrement dit, vous n'avez pas besoin d'utiliser un autre moteur pour les données historiques.  

## Exercice 1 :
On souhaite développer pour une entreprise industrielle une application Spark qui traite les incidents de chaque service. Les incidents sont stockés dans un fichier csv.

Le format de données dans les fichiers csv et la suivante :
**Id, titre, description, service, date**

![image](https://github.com/Dembelinho/TP_SPARK_SQL/assets/110602716/c9caeddb-cde1-4702-814f-7a18d88dcfd1)


> Travail à faire :
### 1. Afficher le nombre d’incidents par service.
   
```
public class Exercise1 {
    public static void main(String[] args) {
        SparkSession sparkSession=SparkSession.builder().appName("TP_SPARK_SQL_Exercise1")
                .master("local[*]").getOrCreate();
        Dataset<Row> dataset1=sparkSession.read()
                .option("header", "true")
                .option("multiline",true)
                .csv("C:/Work/Big Data/untitled/src/main/resources/incidents.csv");
        dataset1.createOrReplaceTempView("incidents");
        sparkSession.sql("select * from incidents where service like 'IT'").show(); 
        dataset1.groupBy("service").agg(functions.count("Id")
                .alias("NombreIncidents")).show(); 
    }
}
```
_**Resultat d'Execution :**_

![exe2](https://github.com/Dembelinho/TP_SPARK_SQL/assets/110602716/081fdb0b-1f15-494a-9fb2-fcc963ab6829)

### 2. Afficher les deux années où il a y avait plus d’incidents.

```
public class Exercise2 {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("TopYears")
                .master("local[*]")  
                .getOrCreate();
        Dataset<Row> incidentsDF = spark.read()
                .option("header", true)
                .option("inferSchema",true)
                .csv("C:/Work/Big Data/untitled/src/main/resources/incidents.csv");
        incidentsDF = incidentsDF.withColumn("Year", year(col("date")));
        Dataset<Row> incidentsParAnnee = incidentsDF.groupBy("Year")
                .agg(count("Id").alias("NombreIncidents"));
        incidentsParAnnee = incidentsParAnnee.orderBy(col("NombreIncidents").desc());
        incidentsParAnnee.show(2); 
    }
}
```
_**Resultat d'Execution :**_ 

![exe3](https://github.com/Dembelinho/TP_SPARK_SQL/assets/110602716/ebb3bdfd-4f5d-453d-8128-b1bdd4744d0c)

## Exercice 2 :

L’hôpital national souhaite traiter ces données au moyen d’une application Spark d’une manière parallèle est distribuée. L’hôpital possède des données stockées dans une base de données relationnel et des fichiers csv. 
L’objectif est de traiter ces données en utilisant Spark SQL à travers les APIs DataFrame et Dataset pour extraire des informations utiles afin de prendre des décisions.

**Traitement de données stockées dans Mysql**

L’hôpital possède une application web pour gérer les consultations de ces patients, les données sont stockées dans une base de données MYSQL nommée DB_HOPITAL, qui contient trois tables PATIENTS, MEDECINS et CONSULTATIONS.

**_Table PATIENTS_**

![patients table](https://github.com/Dembelinho/TP_SPARK_SQL/assets/110602716/4293f4d3-9dda-4fec-a259-dad38dd2b94f)

**_Table MEDECINS_**

![medecins table](https://github.com/Dembelinho/TP_SPARK_SQL/assets/110602716/bcbb1d0a-7e74-40f6-aa7e-2eaa0211cc99)

**_Table CONSULTATIONS_**

![consul table](https://github.com/Dembelinho/TP_SPARK_SQL/assets/110602716/b2412b0a-2ecc-4efc-b142-ea03571572da)

> répondez aux questions suivantes :
- Afficher le nombre de consultations par jour.
- Afficher le nombre de consultation par médecin. Le format d’affichage est le suivant :  
**NOM | PRENOM | NOMBRE DE CONSULTATION**
- Afficher pour chaque médecin, le nombre de patients qu’il a assisté.

```
public class SparkHospital {
    public static void main(String[] args) {
        SparkSession ss=SparkSession.builder().appName("DB_HOSPITAL SQL Spark").master("local[*]").getOrCreate();
        Dataset<Row> consultationsDF = ss.read().format("jdbc")
                .option("driver","com.mysql.cj.jdbc.Driver")
                .option("url","jdbc:mysql://localhost:3306/DB_HOPITAL")
                .option("query","SELECT * FROM `CONSULTATIONS` WHERE 1") 
                .option("user","root")
                .option("password","")
                .load();
        consultationsDF.groupBy("DATE_CONSULTATION").count().show(); 
        Dataset<Row> medecinsDF = ss.read().format("jdbc")
                .option("driver","com.mysql.cj.jdbc.Driver")
                .option("url","jdbc:mysql://localhost:3306/DB_HOPITAL")
                .option("query","SELECT * FROM `MEDECINS` WHERE 1") 
                .option("user","root")
                .option("password","")
                .load();
        Dataset<Row> jDF = consultationsDF.join(medecinsDF, consultationsDF.col("ID_MEDECIN")
                .equalTo(medecinsDF.col("ID")));
        Dataset<Row> consulCountByMed = jDF.groupBy("NOM", "PRENOM").count()
                .withColumnRenamed("count", "NB_CONSULTATION");
        consulCountByMed.show(); 
        Dataset<Row> patientsDF = ss.read().format("jdbc")
                .option("driver","com.mysql.cj.jdbc.Driver")
                .option("url","jdbc:mysql://localhost:3306/DB_HOPITAL")
                .option("query","SELECT * FROM `PATIENTS` WHERE 1") 
                .option("user","root")
                .option("password","")
                .load();
        Dataset<Row> patCountByMed = jDF.groupBy("NOM", "PRENOM")
                .agg(org.apache.spark.sql.functions.countDistinct("ID_PATIENT").alias("NB_PATIENTS"));
        patCountByMed.show();
    }
}
```

_**le nombre de consultations par jour**_

![exe4](https://github.com/Dembelinho/TP_SPARK_SQL/assets/110602716/6bdda7fe-34f4-42ef-89e6-834049429d74)

**_le nombre de consultations par médecin_**

![exe5](https://github.com/Dembelinho/TP_SPARK_SQL/assets/110602716/4c8ce1b6-a38d-4f62-b25e-3eb5199fff32)

_**le nombre de patients par médecin**_

![exe6](https://github.com/Dembelinho/TP_SPARK_SQL/assets/110602716/664c23b9-1e8c-48fb-8278-3d4a6a6f5505)


