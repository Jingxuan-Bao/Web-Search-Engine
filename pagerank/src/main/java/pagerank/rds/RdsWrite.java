package pagerank.rds;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/**
 * Run this program, write the pagerank result into the rds databse
 *             The whole process : RdsRead(get txt data from rds database) ->
 *                                 PageRank(calculate pr value for each url) ->
 *                                 RdsWrite(write the pagerank result into rds database) ->
 *                                 then ready for web search engine
 */
public class RdsWrite {

    private static final String connectionUrl = "jdbc:mysql://cis555db.cpe2gquyawpv.us-east-1.rds.amazonaws.com:3306/cis555?user=admin&password=cis45555";
    private static final String prtable = "Pagerank";


    public static void main(String[] args) {

        String schemaString = "url value";
        // Create a SparkSession
        SparkSession spark = SparkSession
                .builder()
                .appName("pagewriter")
                .master("local[*]") // remove this line if run on EMR cluster
                .getOrCreate();
        // Connect to database using JDBC DriverManager interface
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (Exception e) {
            e.printStackTrace();
        }

        Dataset<Row> PageRes = spark.read().text("./PageRankresult");
        PageRes.createOrReplaceTempView("pageres");
        //PageRes.show();
        Dataset<Row> splitres = spark.sql("select split(value, '\\\\t')[0] as url, split(value, '\\\\t')[1] as value from pageres");
        splitres.show();

        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "admin");
        connectionProperties.put("password", "cis45555");


        splitres.write()
                .format("jdbc")
                .option("url", connectionUrl)
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("dbtable", prtable)
                .mode("overwrite")
                .save();
    }
}
