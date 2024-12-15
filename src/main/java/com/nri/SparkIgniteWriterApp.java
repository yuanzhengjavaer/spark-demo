package com.nri;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.spark.IgniteDataFrameSettings;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import static org.apache.ignite.internal.util.IgniteUtils.resolveIgnitePath;

public class SparkIgniteWriterApp {

    private static final String CONFIG = "config/example-default-client.xml";

    public static void main(String[] args) {
        Ignite ignite = Ignition.start(CONFIG);

        SparkSession spark = SparkSession
                .builder()
                .appName("DFWriter")
                .master("local")
                .config("spark.executor.instances", "2")
                .getOrCreate();

        Logger.getRootLogger().setLevel(Level.OFF);
        Logger.getLogger("org.apache.ignite").setLevel(Level.OFF);


        Dataset<Row> peopleDF = spark.read().format("org.apache.spark.sql.execution.datasources.json.JsonFileFormat")
                .load(resolveIgnitePath("data/person.json").getAbsolutePath());

        System.out.println("JSON file contents:");

        peopleDF.show();

        System.out.println("Writing DataFrame to Ignite.");

        peopleDF.write()
                .format(IgniteDataFrameSettings.FORMAT_IGNITE())
                .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE(), CONFIG)
                .option(IgniteDataFrameSettings.OPTION_TABLE(), "people")
                .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS(), "id")
                .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PARAMETERS(), "template=replicated")
                .mode(SaveMode.Append)
                .option(IgniteDataFrameSettings.OPTION_STREAMER_ALLOW_OVERWRITE(),true)
                .save();

        System.out.println("Done!");

        Ignition.stop(false);
    }
}
