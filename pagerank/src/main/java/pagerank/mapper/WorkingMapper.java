package pagerank.mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * WorkingMapper : Do pagerank, base on the total out links of the source link, send the key (outlink),
 * value (pr value from the source link) to reduce
 */
public class WorkingMapper extends Mapper<Object, Text, Text, Text>{

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        String[] split1 = line.split("\\^");
        String prstr = split1[1];
        float pr = Float.parseFloat(prstr);

        String[] split2 = split1[0].split("\\$");
        if(split2 == null || split2.length < 2) {
            return;
        }
        String totaledges = split2[1];
        float total = Float.parseFloat(totaledges);


        String keyindex = split2[0].split("\\t")[0];
        String edges = split2[0].split("\\t")[1];

        String[] links = edges.split("\\@");

        for(String edge : links) {
            float outres = pr/total;
            context.write(new Text(edge), new Text(String.valueOf(outres)));
        }

        context.write(new Text(keyindex), new Text(split1[0].split("\\t")[1]));
    }
}
