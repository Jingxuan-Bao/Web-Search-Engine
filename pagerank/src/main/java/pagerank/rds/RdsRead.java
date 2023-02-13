package pagerank.rds;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import pagerank.config.PagerankConfig;

import java.io.File;
import java.io.IOException;

import static org.apache.commons.io.FileUtils.deleteDirectory;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat_ws;

/**
 * Run this program, get the links data from the rds database
 *             The whole process : RdsRead(get txt data from rds database) ->
 *                                 PageRank(calculate pr value for each url) ->
 *                                 RdsWrite(write the pagerank result into rds database) ->
 *                                 then ready for web search engine
 */
public class RdsRead {

    private static final String connectionUrl = "jdbc:mysql://cis555db.cpe2gquyawpv.us-east-1.rds.amazonaws.com:3306/cis555?user=admin&password=cis45555";
    private static final String linksTable = "Links2";
    private static final int partitions = 2;

    public static void main(String[] args) {


        // Create a SparkSession
        SparkSession spark = SparkSession
                .builder()
                .appName("pagereader")
                .master("local[*]") // remove this line if run on EMR cluster
                .getOrCreate();
        // Connect to database using JDBC DriverManager interface
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (Exception e) {
            e.printStackTrace();
        }
        // Create a dataframe for "Documents" table
        Dataset<Row> linksDF = spark
                .read()
                .format("jdbc")
                .option("url", connectionUrl)
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("dbtable", linksTable)
                .load()
                .repartition(partitions);
        linksDF.createOrReplaceTempView("links");
        Dataset<Row> rows = linksDF.select(concat_ws(" ", col("src_url"), col("dest_url")));
        //Dataset<Row> rows = spark.sql("SELECT src_id, dest_id FROM links");
        rows.show();
        //rows.write().text("output");

        try {
            deleteDirectory(new File(PagerankConfig.INPUT_DIR));
        }
        catch (IOException e) {
            System.err.println("origin the dir failure");
        }

        rows.coalesce(1).write().text("inputfile");
        //rows.write().option("compression", "gzip").text("output_compressed");
        spark.close();
    }
}
