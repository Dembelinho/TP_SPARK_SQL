package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;

public class Main {
    public static void main(String[] args) {
        SparkSession sparkSession=SparkSession.builder().appName("TP_SPARK_SQL").master("local[*]").getOrCreate();
        Dataset<Row> dataset1=sparkSession.read().option("multiline",true).json("C:/Work/Big Data/untitled/prod.json");
        //dataset1.show();
        dataset1.printSchema();
        //dataset1.select("name").show();
        //dataset1.select(col("name").alias("Name of product")).show();
        // dataset1.orderBy(col("name").asc()).show();
        //dataset1.groupBy(col("name")).count().show();
        //dataset1.limit(2).show();
        // dataset1.filter(col("price").gt(19000)).show();
        //dataset1.filter(col("name").equalTo("Dell").and(col("price").gt(17000))).show();
        // dataset1.filter("name like 'Dell' and price>17000").show();
        dataset1.createOrReplaceTempView("products");
        sparkSession.sql("select * from products where name like 'DELL'").show();

    }
}