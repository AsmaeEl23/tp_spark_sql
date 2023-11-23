package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class App2 {
    public static void main(String[] args) {
        SparkSession ss=SparkSession.builder().appName("TP SPARK SQL").master("local[*]").getOrCreate();
        //Les noms des tables dans mon base de donnee : patients, 	medecins,consultations

        //- Afficher le nombre de consultations par jour.
        Dataset<Row> df1=ss.read().format("jdbc")
                .option("driver","com.mysql.jdbc.Driver")
                .option("url","jdbc:mysql://localhost:3306/mastersdia")
                .option("user","root")
                //.option("dbtable","patients")
                .option("query","SELECT `DATE_CONSULTATION`,COUNT(*) as consultations " +
                        " FROM `consultations` GROUP BY `DATE_CONSULTATION`  ")
                .option("password","")
                .load();
        df1.show();
        //- Afficher le nombre de consultation par médecin. Le format d’affichage est le suivant :
        //NOM | PRENOM | NOMBRE DE CONSULTATION
        Dataset<Row> df2=ss.read().format("jdbc")
                .option("driver","com.mysql.jdbc.Driver")
                .option("url","jdbc:mysql://localhost:3306/mastersdia")
                .option("user","root")
                .option("query","select m.nom,m.prenom,count(*) as nombre_de_consultation " +
                        "from consultations c JOIN medecins m ON c.ID_MEDECIN=m.ID " +
                        "group by m.NOM,m.PRENOM")
                .option("password","")
                .load();
        df2.show();

        //- Afficher pour chaque médecin, le nombre de patients qu’il a assisté.
        Dataset<Row> df3=ss.read().format("jdbc")
                .option("driver","com.mysql.jdbc.Driver")
                .option("url","jdbc:mysql://localhost:3306/mastersdia")
                .option("user","root")
                .option("query","select m.nom as le_nom_de_médecin ,m.prenom as le_prénom_de_médecin," +
                        "p.NOM  as le_nom_de_patient,p.PRENOM as le_prenom_de_patient " +
                        "from consultations c JOIN medecins m ON c.ID_MEDECIN=m.ID " +
                        "join patients p ON c.ID_PATIENT=p.ID ")
                .option("password","")
                .load();
        df3.show();
    }
}

