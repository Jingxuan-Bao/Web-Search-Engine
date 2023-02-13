package cis555;

import java.util.List;

import org.apache.spark.sql.SparkSession;
import static spark.Spark.*;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * A server listening on port 8000,
 * implemented using Spark Java
 */
public class Server {

    private static final String connectionUrl = "jdbc:mysql://cis555db.cpe2gquyawpv.us-east-1.rds.amazonaws.com:3306/cis555?user=admin&password=cis45555";

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("SearchEngine").master("local[*]").getOrCreate();

        port(8000);

        // prevent cors block
        before((req, res) -> res.header("Access-Control-Allow-Origin", "*"));

        // convert the list of results returned by search engine to JSON
        ObjectMapper objectMapper = new ObjectMapper();
        get("/search", (req, res) -> {
            String query = req.queryParams("query");
            List<Result> results = SearchEngine.getResults(spark, connectionUrl, query);
            return objectMapper.writeValueAsString(results);
        });

        awaitInitialization();
        System.out.print("Starting server...");
    }
}
