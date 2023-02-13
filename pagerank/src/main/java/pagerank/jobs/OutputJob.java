package pagerank.jobs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pagerank.PageRank;
import pagerank.mapper.OutputMapper;
import pagerank.mapper.PrepareMapper;
import pagerank.reduce.OutputReduce;
import pagerank.reduce.PrepareReduce;

import java.io.IOException;

/**
 * OutputJob config (write into rds)
 */
public class OutputJob implements runJob{
    //static Logger logger = LoggerFactory.getLogger(PrepareJob.class);
    Job job;

    public OutputJob() {
        try {
            this.job = Job.getInstance(PageRank.getconf(), "PrepareJob");
        }
        catch (IOException e) {
            //logger.error("Define Output Job Error");
        }
    }

    @Override
    public int run(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        job.setJarByClass(PrepareJob.class);

        job.setMapperClass(OutputMapper.class);
        job.setReducerClass(OutputReduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    @Override
    public Job getJob() {
        return this.job;
    }
}
