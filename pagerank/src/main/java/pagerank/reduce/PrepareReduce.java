package pagerank.reduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * PrepareReduce, build a relation of src link to the combination of dest links,
 * the total out link number, and the init pagerank val (1)
 */
public class PrepareReduce extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuilder sb = new StringBuilder();
        int count = 0;
        // @ : the connect between dest url
        for(Text val : values) {
            count ++;
            sb.append(val.toString()).append("@");
        }

        String res = sb.toString().substring(0, sb.length() - 1);
        // $ : the number of total outlinks
        res += "$";
        res += String.valueOf(count);
        // ^ : the initial of pagerank val
        res += "^";
        //float pr = 1/Float.valueOf(count);
        // 1 for initial pr val
        float pr = 1;
        res += String.valueOf(pr);
        context.write(key, new Text(res));

        System.out.println("finish word count");
    }
}
