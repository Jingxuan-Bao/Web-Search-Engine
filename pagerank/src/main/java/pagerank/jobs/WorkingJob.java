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
import pagerank.mapper.PrepareMapper;
import pagerank.mapper.WorkingMapper;
import pagerank.reduce.PrepareReduce;
import pagerank.reduce.WorkingReduce;

import java.io.IOException;

/**
 * WorkingJob config (calculate pagerank value each iteration
 */
public class WorkingJob implements runJob{
    //static Logger logger = LoggerFactory.getLogger(WorkingJob.class);
    Job job;
    public static final float decay = (float) 0.85;

    public WorkingJob() {
        try {
            this.job = Job.getInstance(PageRank.getconf(), "WorkingJob");
        }
        catch (IOException e) {
            //logger.error("Define Working Job Error");
        }
    }

    @Override
    public int run(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        job.setJarByClass(WorkingJob.class);

        job.setMapperClass(WorkingMapper.class);
        job.setReducerClass(WorkingReduce.class);

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
