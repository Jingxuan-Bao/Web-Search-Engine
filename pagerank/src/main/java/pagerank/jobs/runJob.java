package pagerank.jobs;

import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
/**
 * The runjob interface
 */
public interface runJob {

    public int run(String inputPath, String outputPath)
            throws IOException, ClassNotFoundException, InterruptedException;

    public Job getJob();
}
