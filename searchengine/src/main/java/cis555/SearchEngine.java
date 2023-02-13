package cis555;

import java.sql.*;
import java.util.*;
import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.api.java.JavaPairRDD;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import org.tartarus.snowball.ext.englishStemmer;

/**
 * Combine results provided by indexer and pagerank to
 * get a ultimate ranking of the relevant documents
 */
public class SearchEngine {
    // static final variables of RDS table names
    private static final String idfsTable = "IDFs";
    private static final String invertedIndexTable = "Inverted_Index";
    private static final String PageRank = "Pagerank";
    private static final String Documents = "Documents2";

    SparkConf conf = new SparkConf().setAppName("SearchEngine").setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(conf);

    /***
     * 
     * @param spark         - a SparkSession
     * @param connectionUrl - a RDS connection url
     * @param query         - search terms entered by the client
     * @return a list of documents with relevant information about the search terms
     */
    public static List<Result> getResults(SparkSession spark, String connectionUrl, String query) {
        // STEP 1: Use results provided by the indexer to calcuate cosine similarity
        // between the search terms and documents containing these search terms

        // similar to indexer, first calculate term frequency within the search terms
        englishStemmer stemmer = new englishStemmer();
        Map<String, Integer> tfMap = new HashMap<>();
        int maxFreq = 0; // a variable to help with normalization
        Map<String, Integer> vecIndexMap = new HashMap<>();
        int vecIndex = 0;

        // Remove whitespaces, remove punctuations, remove numbers,
        // remove non-US-ASCII characters, erase null characters,
        // and find stemmed words to determine whether a search term is valid
        StringBuilder termTuple = new StringBuilder(); // a variable to help with sql query
        termTuple.append("(");
        for (String term : query.split("[\\p{Punct}\\s]+")) {
            term = term.toLowerCase().replaceAll("[0-9]", "").replaceAll("[^\\x00-\\x7f]", "").replaceAll("\u0000", "");
            term = term.trim();
            if (term.isEmpty() || term.length() > 45) {
                continue;
            }
            stemmer.setCurrent(term);
            if (stemmer.stem()) {
                term = stemmer.getCurrent();
            }
            int freq = tfMap.getOrDefault(term, 0) + 1;
            tfMap.put(term, freq);
            maxFreq = Math.max(freq, maxFreq);
            if (!vecIndexMap.containsKey(term)) {
                vecIndexMap.put(term, vecIndex++);
            }
            termTuple.append("'").append(term).append("',");
        }
        if (termTuple.length() > 1) {
            termTuple.deleteCharAt(termTuple.length() - 1);
            termTuple.append(")");
        } else {
            // all search terms are invalid, no matching docs, return null
            return null;
        }

        double[] vec = new double[vecIndex]; // a variable to help with calculating similarity
        try {
            // Connect to database
            // Find idf for search terms, calcuate tf*idf to put into an vector array
            Class.forName("com.mysql.cj.jdbc.Driver");
            Connection conn = DriverManager.getConnection(connectionUrl);
            Statement s = conn.createStatement();
            ResultSet rs = s.executeQuery("SELECT * FROM " + idfsTable + " WHERE term IN " + termTuple.toString());
            if (rs.next()) {
                while (rs.next()) {
                    String term = rs.getString(1);
                    double idf = rs.getDouble(2);
                    double normalizedFrequency = ((double) tfMap.get(term)) / maxFreq;
                    vec[vecIndexMap.get(term)] = 0.5 + (1 - 0.5) * normalizedFrequency * idf;
                }
            } else {
                // all search terms are not in database, no matching docs, return null
                return null;
            }
        } catch (Exception e) {
            e.printStackTrace();
            // sql exception, no matching docs, return null
            return null;
        }

        // Read from "Inverted_Index" table to get tf*idf for documents containing
        // the search term, and put into another vector array
        Dataset<Row> invertedIndexDF = spark.read().format("jdbc")
                .option("url", connectionUrl)
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("query", "SELECT * FROM " + invertedIndexTable + " WHERE term IN " + termTuple.toString())
                .load();
        JavaPairRDD<String, Iterable<Tuple2<Integer, Double>>> docWeightPairs = invertedIndexDF
                .toJavaRDD().mapToPair(row -> new Tuple2<>(row.getAs("url").toString(),
                        new Tuple2<>(vecIndexMap.get(row.getAs("term")), (double) row.getAs("weight"))))
                .groupByKey();

        // Using the two vector arrays to calculate cosine similarity between them
        JavaPairRDD<Double, String> cosineSimilarityPairs = docWeightPairs.mapToPair(docWeightPair -> {
            double[] docVec = new double[vec.length];
            for (Tuple2<Integer, Double> pair : docWeightPair._2) {
                docVec[pair._1] = pair._2;
            }
            double sumProduct = 0.0, sumASq = 0.0, sumBSq = 0.0;
            for (int i = 0; i < vec.length; i++) {
                sumProduct += vec[i] * docVec[i];
                sumASq += vec[i] * vec[i];
                sumBSq += docVec[i] * docVec[i];
            }
            double sqrt = Math.sqrt(sumASq) * Math.sqrt(sumBSq);
            if (sqrt == 0.0) { // prevent division by zero
                sqrt = 0.00001;
            }
            double cosineSimilarity = sumProduct / sqrt;
            return new Tuple2<>(cosineSimilarity, docWeightPair._1);
        });

        // Obtain documents ranked by their cosine similarity value
        JavaPairRDD<String, Double> similarityDocs = cosineSimilarityPairs
                .sortByKey(false).mapToPair(cosineSimilarityPair -> {
                    return new Tuple2<>(cosineSimilarityPair._2, cosineSimilarityPair._1);
                });

        // STEP 2: Combine cosine similarity value with pagerank value
        // to get a ultimate ranking of the documents, also retrieve
        // information about the document title and document snippet
        StringBuilder urlTuple = new StringBuilder(); // a variable to help with sql query
        urlTuple.append("(");
        for (Tuple2<String, Double> similarityDoc : similarityDocs.take(3000)) {
            urlTuple.append("'").append(similarityDoc._1).append("',");
        }
        if (urlTuple.length() > 1) {
            urlTuple.deleteCharAt(urlTuple.length() - 1);
            urlTuple.append(")");
        } else {
            return null;
        }

        // Retrieve document information for urls that are in similarityDocs,
        // store in a hashmap
        Map<String, String> documentMap = new HashMap<>();
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            Connection conn = DriverManager.getConnection(connectionUrl);
            Statement s = conn.createStatement();
            ResultSet rsDocument = s
                    .executeQuery("SELECT url, content FROM " + Documents + " WHERE url IN " + urlTuple.toString());
            if (rsDocument.next()) {
                while (rsDocument.next()) {
                    String url = rsDocument.getString(1);
                    String content = rsDocument.getString(2);
                    documentMap.put(url, content);
                }
            } else {
                return null;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

        // Retrieve pagerank information for urls that are in similarityDocs,
        // store in a hashmap
        Map<String, Double> pageRankMap = new HashMap<>();
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            Connection conn = DriverManager.getConnection(connectionUrl);
            Statement s = conn.createStatement();
            ResultSet rsPageRank = s.executeQuery("SELECT * FROM " + PageRank + " WHERE url IN " + urlTuple.toString());
            if (rsPageRank.next()) {
                while (rsPageRank.next()) {
                    String url = rsPageRank.getString(1);
                    double rank = rsPageRank.getDouble(2);
                    pageRankMap.put(url, rank);
                }
            } else {
                return null;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

        List<Result> resultList = new ArrayList<>(); // a list of documents to be returned
        for (Tuple2<String, Double> similarityDoc : similarityDocs.take(3000)) {
            String url = similarityDoc._1;
            if (!documentMap.containsKey(url) || !pageRankMap.containsKey(url)) {
                continue;
            }
            // detemine a ultimate ranking value
            double similarity = similarityDoc._2;
            double rankValue = pageRankMap.get(url);
            double harmonicMean = 0.0;
            if (similarity == 0.0) { // prevent division by zero
                similarity = 0.00001;
            }
            if (rankValue == 0.0) {
                rankValue = 0.00001;
            }
            harmonicMean = 2 / (1 / similarity + 1 / rankValue);
            // get information about title and snippet of the documents
            String content = documentMap.get(url);
            Document document = Jsoup.parse(content);
            Elements elements = document.getElementsByTag("title");
            String title = ""; // title
            if (!elements.isEmpty()) {
                title = elements.get(0).text();
            }
            StringBuilder snippet = new StringBuilder(); // snippet
            elements = document.getElementsByTag("p");
            if (!elements.isEmpty()) {
                int index = 0;
                while (index < elements.size() && snippet.length() < 60) {
                    snippet.append(elements.get(index++).text()).append(" ");
                }
            }
            Result result = new Result(url, title, snippet == null ? "" : snippet.toString(), harmonicMean);
            resultList.add(result);
        }

        // Sort the list based on the ultimate ranking value
        // and return the list
        Collections.sort(resultList, new Comparator<Result>() {
            @Override
            public int compare(Result r1, Result r2) {
                if (r1.getRank() == r2.getRank()) {
                    return 0;
                }
                return r1.getRank() > r2.getRank() ? -1 : 1;
            }
        });
        return resultList;
    }
}
