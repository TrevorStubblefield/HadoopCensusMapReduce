package cs455.hadoop.Q4;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives word, list<count> pairs.
 * Sums up individual counts per given word. Emits <word, total count> pairs.
 */
public class Q4Reducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double total = 0;
        double urban = 0;
        double rural = 0;
        // calculate the total count
        for(Text val : values){
            String[] split = val.toString().split(" ");
            total += Integer.parseInt(split[2]);
            urban += Integer.parseInt(split[0]);
            rural += Integer.parseInt(split[1]);
        }
        context.write(new Text(key.toString() + " Urban Houses: "),new Text(""+(urban/total)*100 + "%") );
        context.write(new Text(key.toString() + " Rural Houses: "),new Text(""+(rural/total)*100 + "%") );

    }
}
