package pagerank.mapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * OutputMapper : remove the links information, only save pagerank value for write into
 */
public class OutputMapper extends Mapper<Object, Text, Text, Text> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        String id = line.split("\t")[0];

        String pr = line.split("\\^")[1];

        context.write(new Text(id), new Text(pr));
    }
}
