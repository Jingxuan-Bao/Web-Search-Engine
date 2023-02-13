package cis555;

import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import scala.Tuple2;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.api.java.JavaPairRDD;
import static org.apache.spark.sql.functions.col;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.tartarus.snowball.ext.englishStemmer;

/**
 * Use data from the "Documents" table in RDS to calcualte tf, idf and weight to
 * create a "IDFs" table and a "Inverted_Index" table to help with ranking
 */
public class Indexer {
    // static final variables of RDS url and table names
    private static final String connectionUrl = "jdbc:mysql://cis555db.cpe2gquyawpv.us-east-1.rds.amazonaws.com:3306/cis555?user=admin&password=cis45555";
    private static final String documentsTable = "Documents";
    private static final String idfsTable = "IDFs";
    private static final String invertedIndexTable = "Inverted_Index";

    public static void main(String[] args) {
        // Create a SparkSession and
        // connect to database using JDBC DriverManager interface
        SparkSession spark = SparkSession.builder().appName("Indexer").master("local[*]").getOrCreate();
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Create a dataframe for "Documents" table
        Dataset<Row> documentsDF = spark.read().format("jdbc")
                .option("url", connectionUrl)
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("dbtable", documentsTable).load().repartition(10);

        // Convert each Row in dataframe to a tuple of (url, content)
        JavaPairRDD<String, String> urlContentPairs = documentsDF
                .toJavaRDD().mapToPair(row -> new Tuple2<>(row.getAs("url"), row.getAs("content")));

        // Read stopwords from a txt file
        Set<String> stopwords = new Stopwords().readStopwords();

        // Convert (url, content) to (term, (url, term frequency within a document))
        JavaPairRDD<String, Tuple2<String, Double>> tfWithinDocumentPairs = urlContentPairs
                .flatMapToPair(urlContentPair -> {
                    String url = urlContentPair._1;
                    String content = urlContentPair._2;
                    Document document = Jsoup.parse(content);

                    englishStemmer stemmer = new englishStemmer();
                    Map<String, Integer> tfMap = new HashMap<>();
                    int maxFreq = 0; // a variable to help with normalization

                    // Remove whitespaces, remove punctuations, remove numbers,
                    // remove non-US-ASCII characters, erase null characters,
                    // remove stopwords and find stemmed words
                    for (String str : document.text().split("[\\p{Punct}\\s]+")) {
                        String term = str.toLowerCase().replaceAll("[0-9]", "")
                                .replaceAll("[^\\x00-\\x7f]", "")
                                .replaceAll("\u0000", "");
                        term = term.trim();
                        if (term.isEmpty() || term.length() > 45) { // validate term length
                            continue;
                        }
                        if (stopwords.contains(term)) { // validate whether term is a stopword
                            continue;
                        }
                        stemmer.setCurrent(term);
                        if (stemmer.stem()) {
                            term = stemmer.getCurrent();
                        }
                        int freq = tfMap.getOrDefault(term, 0) + 1;
                        tfMap.put(term, freq);
                        maxFreq = Math.max(freq, maxFreq);
                    }
                    List<Tuple2<String, Tuple2<String, Double>>> tfList = new ArrayList<>();
                    for (String term : tfMap.keySet()) {
                        int freq = tfMap.get(term);
                        double normalizedFrequency = 0.5 + (1 - 0.5) * ((double) freq / maxFreq);
                        tfList.add(new Tuple2<>(term, new Tuple2<>(url, normalizedFrequency)));
                    }
                    return tfList.iterator();
                });

        // Convert (term, (url, term frequency within a document))
        // to (term, term frequency across documents)
        JavaPairRDD<String, Integer> tfAcrossDocumentsPairs = tfWithinDocumentPairs
                .mapToPair(tfPair -> new Tuple2<>(tfPair._1, 1))
                .aggregateByKey(0, (sum, value) -> sum + 1, (sum1, sum2) -> sum1 + sum2);

        // Convert (term, term frequency across documents) to (term, idf)
        // using the formula idf = log(N/(df + 1)), add 1 to prevent division by zero
        long N = documentsDF.count();
        JavaPairRDD<String, Double> idfPairs = tfAcrossDocumentsPairs
                .mapToPair(tfPair -> new Tuple2<>(tfPair._1, Math.log((double) N / (tfPair._2 + 1))));

        // Convert (term, idf) to (term, (url, td*idf))
        JavaPairRDD<String, Tuple2<String, Double>> weightPairs = tfWithinDocumentPairs
                .join(idfPairs).mapToPair(tfPair -> new Tuple2<>(tfPair._1,
                        new Tuple2<>(tfPair._2._1._1, tfPair._2._1._2 * tfPair._2._2)));

        // Add idf data to "IDFs" table in database
        // table stores information about term and its corresponding idf
        Dataset<Row> idfsDF = spark
                .createDataset(JavaPairRDD.toRDD(idfPairs), Encoders.tuple(Encoders.STRING(), Encoders.DOUBLE()))
                .toDF("term", "idf");
        idfsDF.write().format("jdbc")
                .option("url", connectionUrl)
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("dbtable", idfsTable)
                .mode("overwrite").save();

        // Add tf*idf data to "Inverted_Index" table in database
        // table stores information about url, term and its corresponding tf*idf value
        Dataset<Row> tfidfsRawDF = spark
                .createDataset(JavaPairRDD.toRDD(weightPairs),
                        Encoders.tuple(Encoders.STRING(),
                                Encoders.tuple(Encoders.STRING(), Encoders.DOUBLE())))
                .toDF("term", "tuple");
        Dataset<Row> tfidfsDF = tfidfsRawDF.select(
                col("term").as("term"),
                col("tuple._1").as("url"),
                col("tuple._2").as("weight"));
        tfidfsDF.write().format("jdbc")
                .option("url", connectionUrl)
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("dbtable", invertedIndexTable)
                .mode("overwrite").save();

        // Close SparkSession
        spark.close();
    }
}
