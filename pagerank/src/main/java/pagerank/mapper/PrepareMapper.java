package pagerank.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * PrepareMapper : filter the links, send to reduce as (key : source link, val :
 * dst link)
 */
public class PrepareMapper extends Mapper<Object, Text, Text, Text> {

    // static Logger logger = LoggerFactory.getLogger(PrepareMapper.class);

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        System.out.println(line);

        String[] splits = line.split(" ");

        /*
         * remove invalid url
         */
        if (!splits[0].startsWith("http") || !splits[1].startsWith("http")) {
            return;
        }
        /*
         * remove url with special
         */
        if (splits[0].contains("$") || splits[1].contains("$") || splits[0].contains("@") || splits[1].contains("@")) {
            return;
        }

        if (splits.length < 2) {
            System.out.println(line);
            return;
        }

        else {
            context.write(new Text(splits[0]), new Text(splits[1]));
        }
    }
}
