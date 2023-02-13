package pagerank.reduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * OutputReduce, get the final pagerank value, and write into a txt file, ready for RdsWrite
 */
public class OutputReduce extends Reducer<Text, Text, Text, Text> {


    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuilder sb = new StringBuilder();
        float prsum = 0;
        for(Text val : values) {
            prsum += Float.parseFloat(val.toString());
        }
        sb.append(String.valueOf(prsum));
        context.write(key, new Text(sb.toString()));
    }
}
