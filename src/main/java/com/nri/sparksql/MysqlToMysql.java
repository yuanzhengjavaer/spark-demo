package com.nri.sparksql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class MysqlToMysql {

    public static void main(String[] args) {

        // 注册JDBC驱动
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("mysql2Mysql")
                .getOrCreate();

        // Note: JDBC loading and saving can be achieved via either the load/save or jdbc methods
        // Loading data from a JDBC source
        Dataset<Row> jdbcDF = spark.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://218.78.35.94:3306/ignite_persis?useSSL=false")
                .option("dbtable", "maintenance")
                .option("user", "root")
                .option("password", "65316817")
                .load();

        // Saving data to a JDBC source
        jdbcDF.write()
                .format("jdbc")
                .option("url", "jdbc:mysql://218.78.35.94:3306/myTest?useSSL=false")
                .option("dbtable", "maintenance")
                .option("user", "root")
                .option("password", "65316817")
                .mode(SaveMode.Append)
                .save();
    }
}
