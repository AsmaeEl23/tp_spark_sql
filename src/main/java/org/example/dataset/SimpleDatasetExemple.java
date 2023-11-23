package org.example.dataset;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

public class SimpleDatasetExemple {
    public static void main(String[] args) {
        SparkSession ss=SparkSession.builder().appName("TP SPARK SQL").master("local[*]").getOrCreate();
        List<Product> products=new ArrayList<>();
        products.add(new Product("Dell",12000.0,7));
        products.add(new Product("Mac",55000.0,2));
        products.add(new Product("HP",15000.0,10));

        //Pour la serialisation
        Encoder<Product> productEncoder= Encoders.bean(Product.class);
        Dataset<Product> productDataset=ss.createDataset(products,productEncoder);
        productDataset.filter((FilterFunction<Product>)  product -> product.getPrice()>12000).show();

        //Dataset
        //Dataset<Row> df1=ss.read().option("multiline",true).json("products.json");
        // df1.show();
        //df1.printSchema();
        //df1.select("name").show();
        //df1.select(col("name").alias("Name of product")).show();
        // df1.orderBy(col("name").asc()).show();
        //df1.groupBy(col("name")).count().show();
        //df1.limit(2).show();
        // df1.filter(col("price").gt(19000)).show();
        //df1.filter(col("name").equalTo("Dell").and(col("price").gt(17000))).show();
        // df1.filter("name like 'Dell' and price>17000").show();
        //------------->Create a view
        //dframe1.createOrReplaceTempView("incidents");
        //ss.sql("select * from incidents ").show();


    }
}
