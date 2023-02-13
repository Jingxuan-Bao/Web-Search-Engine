package cis555;

/**
 * a class to store information about the documents that
 * will be returned by the search engine to the client;
 * stores information about url, the title of the document,
 * a short paragraph from the docuemnt and its rank value
 */
public class Result {
    private String url;
    private String title;
    private String snippet;
    private double rank;

    public Result(String url, String title, String snippet, double rank) {
        this.url = url;
        this.title = title;
        this.snippet = snippet;
        this.rank = rank;
    }

    public String getUrl() {
        return url;
    }

    public String getTitle() {
        return title;
    }

    public String getSnippet() {
        return snippet;
    }

    public double getRank() {
        return rank;
    }
}
