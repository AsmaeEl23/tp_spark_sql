package org.example;
import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.year;


public class App1 {
    public static void main(String[] args) {
        SparkSession ss=SparkSession.builder().appName("TP SPARK SQL").master("local[*]").getOrCreate();
        Dataset<Row> dframe1=ss.read().option("header",true).option("inferSchema",true).csv("incidents.csv");
        //dframe1.show();
        //dframe1.printSchema();
        //dframe1.select(col("price").plus(2000)).show();


        //1. Afficher le nombre d’incidents par service.
        dframe1.groupBy("service").count().show();
        //2. Afficher les deux années où il a y avait plus d’incidents.
        dframe1.groupBy(year(col("date")).alias("year")).count().orderBy(col("count").desc()).limit(2).show();

    }
}