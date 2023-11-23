package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class App2 {
    public static void main(String[] args) {
        SparkSession ss=SparkSession.builder().appName("TP SPARK SQL").master("local[*]").getOrCreate();
        //Les noms des tables dans mon base de donnee : patients, 	medecins,consultations
        Dataset<Row> df=ss.read().format("jdbc")
                .option("driver","com.mysql.jdbc.Driver")
                .option("url","jdbc:mysql://localhost:3306/mastersdia")
                .option("user","root")
                .option("dbtable","patients")
                //.option("query","")
                .option("password","")
                .load();
        //Afficher le schema
        df.printSchema();

        //- Afficher le nombre de consultations par jour.
        Dataset<Row> df1=ss.read().format("jdbc")
                .option("driver","com.mysql.jdbc.Driver")
                .option("url","jdbc:mysql://localhost:3306/mastersdia")
                .option("user","root")
                //.option("dbtable","patients")
                .option("query","")
                .option("password","")
                .load();
        //- Afficher le nombre de consultation par médecin. Le format d’affichage est le suivant :
        //NOM | PRENOM | NOMBRE DE CONSULTATION
        //- Afficher pour chaque médecin, le nombre de patients qu’il a assisté.
    }
}

