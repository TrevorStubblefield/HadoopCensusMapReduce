package cs455.hadoop.Q8;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives word, list<count> pairs.
 * Sums up individual counts per given word. Emits <word, total count> pairs.
 */
public class Q8Combiner extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        int totalPerState = 0;
        int elderlyPerState = 0;

        for (Text val : values){
            String text = val.toString();
            String[] split = text.split(" ");
            String total = split[0];
            String elderly = split[1];

            totalPerState += Integer.parseInt(total);
            elderlyPerState += Integer.parseInt(elderly);
        }

        context.write(new Text(""),new Text(key.toString() + " " +totalPerState + " " + elderlyPerState));


    }
}
