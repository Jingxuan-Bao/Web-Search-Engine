package pagerank.reduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import pagerank.jobs.WorkingJob;

import java.io.IOException;

/**
 * WorkingReduce, calculate the pagerank value each iteration, build the structure ready for next iteration
 */
public class WorkingReduce extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuilder sb = new StringBuilder();
        String outlinks = "";
        float sum = 0;

        for(Text val : values) {
            if(val.toString().contains("$")) {
                outlinks = val.toString();
                continue;
            }
            else {
                sum += Float.parseFloat(val.toString());
            }
        }

        // decay for hogs and self-loops

        float newpr = WorkingJob.decay * sum + (1 - WorkingJob.decay);

        sb.append(outlinks);
        sb.append("^");
        sb.append(String.valueOf(newpr));
        context.write(key, new Text(sb.toString()));
    }
}
