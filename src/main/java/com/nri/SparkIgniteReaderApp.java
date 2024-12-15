package com.nri;


import org.apache.ignite.Ignition;

import org.apache.ignite.spark.IgniteDataFrameSettings;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkIgniteReaderApp {

    private static final String CONFIG = "config/example-default-client.xml";

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("DFReader")
                .master("local")
                .config("spark.executor.instances", "2")
                .getOrCreate();


        System.out.println("Reading data from Ignite table.");

        Dataset<Row> peopleDF = spark.read()
                .format(IgniteDataFrameSettings.FORMAT_IGNITE())
                .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE(), CONFIG)
                .option(IgniteDataFrameSettings.OPTION_TABLE(), "people")
                .load();

        peopleDF.createOrReplaceTempView("people");

        Dataset<Row> sqlDF = spark.sql("SELECT * FROM people WHERE id > 0 AND id < 6");
//       sqlDF.foreach((ForeachFunction<Row>) row -> System.out.println(row));
        sqlDF.show();
        System.out.println("Done!");

        Ignition.stop(false);
    }
}
