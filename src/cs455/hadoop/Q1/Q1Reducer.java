package cs455.hadoop.Q1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives word, list<count> pairs.
 * Sums up individual counts per given word. Emits <word, total count> pairs.
 */
public class Q1Reducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int total = 0;
        int rented = 0;
        int owned = 0;
        // calculate the total count
        for(Text val : values){
            String[] split = val.toString().split(" ");
            total += Integer.parseInt(split[0]);
            total += Integer.parseInt(split[1]);
            owned += Integer.parseInt(split[0]);
            rented += Integer.parseInt(split[1]);
        }
        context.write(new Text(key.toString() + " Percent Owned: "),new Text(""+(((double)owned/total)*100) + "%"));
        context.write(new Text(key.toString() + " Percent Rented: "),new Text(""+(((double)rented/total)*100) + "%"));

    }
}
