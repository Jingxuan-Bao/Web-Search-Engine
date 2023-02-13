package pagerank;

import org.apache.hadoop.conf.Configuration;
import pagerank.config.PagerankConfig;
import pagerank.jobs.OutputJob;
import pagerank.jobs.PrepareJob;
import pagerank.jobs.WorkingJob;
import pagerank.jobs.runJob;

import java.io.File;
import java.io.IOException;

import static org.apache.commons.io.FileUtils.deleteDirectory;

/**
 * Run this program, do the pagerank
 * The whole process : RdsRead(get txt data from rds database) ->
 * PageRank(calculate pr value for each url) ->
 * RdsWrite(write the pagerank result into rds database) ->
 * then ready for web search engine
 */

public class PageRank {
    public static final Configuration conf = new Configuration();

    public static Configuration getconf() {
        return conf;
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        /*
         * delete the directory before
         */
        try {
            deleteDirectory(new File(PagerankConfig.OUTPUT_DIR));
            for (int i = 0; i <= PagerankConfig.TOTAL_ITERATION; i++) {
                String temp = String.valueOf(i);
                deleteDirectory(new File(PagerankConfig.OUTPUT_DIR + temp));
            }
            deleteDirectory(new File(PagerankConfig.RES_DIR));
        } catch (IOException e) {
            System.err.println("origin the dir failure");
        }

        /*
         * do the prepare job, read from the txt file, build the outlink from every
         * link, origin the pr value 1
         */
        runJob prepareJob = new PrepareJob();
        // int prepareres = prepareJob.run(PagerankConfig.INPUT_FILE,
        // PagerankConfig.OUTPUT_DIR + "0");
        int prepareres = prepareJob.run(PagerankConfig.INPUT_DIR, PagerankConfig.OUTPUT_DIR + "0");
        if (prepareres != 0) {
            System.err.println("Bad in PrepareJob !!!");
            System.exit(1);
        }

        /*
         * do the pagerank job, loop n times
         */
        String workinginput = "";
        String workingoutput = "";
        for (int i = 0; i < PagerankConfig.TOTAL_ITERATION; i++) {
            if (i == 0) {
                workinginput = PagerankConfig.OUTPUT_DIR + "0" + PagerankConfig.DATA_FILE;
            } else {
                workinginput = PagerankConfig.OUTPUT_DIR + String.valueOf(i) + PagerankConfig.DATA_FILE;
            }
            workingoutput = PagerankConfig.OUTPUT_DIR + String.valueOf(i + 1);
            runJob workingJob = new WorkingJob();
            int workingres = workingJob.run(workinginput, workingoutput);
            if (workingres != 0) {
                System.err.println("Bad in WorkingJob !!! in num " + String.valueOf(i));
                System.exit(1);
            }
        }

        /*
         * do the final job, get the pr value, and write url id and pr val into final
         * txt file
         */
        String outputpath = PagerankConfig.OUTPUT_DIR + String.valueOf(PagerankConfig.TOTAL_ITERATION);
        runJob outputJob = new OutputJob();
        int outputres = outputJob.run(outputpath, "PageRankresult");
        if (outputres != 0) {
            System.err.println("Bad in outputjob !!!");
            System.exit(1);
        }

    }
}
