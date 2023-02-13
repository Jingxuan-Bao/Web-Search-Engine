package cis555;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Set;
import java.util.HashSet;

/**
 * Read stopwords from a txt file
 */
public class Stopwords {

    // constructor
    public Stopwords() {
    }

    // returns a set of all stopwords in the txt file
    public Set<String> readStopwords() {
        Set<String> stopwords = new HashSet<>();
        try {
            BufferedReader br = new BufferedReader(
                    new InputStreamReader(getClass().getResourceAsStream("/stopwords.txt")));
            String line = null;
            while ((line = br.readLine()) != null) {
                stopwords.add(line);
            }
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return stopwords;
    }
}
