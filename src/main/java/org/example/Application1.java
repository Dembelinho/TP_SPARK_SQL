package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Application1 {
    public static void main(String[] args) {
        SparkSession ss=SparkSession.builder().appName("MySQL Spark").master("local[*]").getOrCreate();
        Dataset<Row> df1 = ss.read().format("jdbc")
                .option("driver","com.mysql.cj.jdbc.Driver")
                .option("url","jdbc:mysql://localhost:3306/MYDB")
                //.option("dbtable","Products") //je peux creer le Dataframe a partir d'une table DB
                .option("query","SELECT * FROM `products` WHERE 1") //comme je peux la creer a partir d'une requete Sql
                .option("user","root")
                .option("password","")
                .load();
        df1.show();
        df1.printSchema();
    }
}
