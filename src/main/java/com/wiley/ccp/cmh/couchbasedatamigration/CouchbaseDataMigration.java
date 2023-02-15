package com.wiley.ccp.cmh.couchbasedatamigration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.EqualTo;

import static com.couchbase.spark.japi.CouchbaseDataFrameReader.couchbaseReader;

public class CouchbaseDataMigration {

    public static void main(String[] args) {

        /*Logger log = Logger.getRootLogger();
        log.setLevel(Level.ERROR);

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);*/

        var spark = SparkSession
                .builder()
                .appName("MigrateCouchbase2Postgres")
                .master("local[*]")
                .config("spark.couchbase.nodes", "localhost")
                .config("spark.couchbase.username", "travel-sample")
                .config("spark.couchbase.password", "g3n3r4l")
                .config("spark.couchbase.bucket." + "travel-sample", "")
                .config("com.couchbase.socketConnect", 300000)
                .config("com.couchbase.connectTimeout", 300000)
                .config("com.couchbase.queryTimeout", 300000)
                .config("com.couchbase.maxRequestLifetime", 300000)
                .config("spark.driver.allowMultipleContext", "true")
                .getOrCreate();

        Dataset<Row> travelDS = couchbaseReader(spark.read()).couchbase(new EqualTo("type", "airline"));
        //System.out.println(travelDS.count());

        travelDS.repartition(4).foreachPartition(each -> {
            try {
                Class.forName("org.postgresql.Driver");

                String url = "jdbc:postgresql://localhost:5432/postgres";
                Properties props = new Properties();
                props.setProperty("user", "postgres");
                props.setProperty("password", "postgres");
                Connection conn = DriverManager.getConnection(url, props);

                PreparedStatement pStmt = conn.prepareStatement("insert into travel_sample values (?,?,?,?,?,?,?,?)");

                while (each.hasNext()) {
                    Row row = each.next();

                    pStmt.setString(1, row.getString(0));
                    pStmt.setString(2, row.getString(1));
                    pStmt.setString(3, row.getString(2));
                    pStmt.setString(4, row.getString(3));
                    pStmt.setString(5, row.getString(4));
                    pStmt.setLong(6, row.getLong(5));
                    pStmt.setString(7, row.getString(6));
                    pStmt.setString(8, row.getString(7));

                    pStmt.executeUpdate();
                }
            } catch (ClassNotFoundException cnfe) {
                cnfe.printStackTrace();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }
}
