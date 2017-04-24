package cs455.hadoop.Q2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives word, list<count> pairs.
 * Sums up individual counts per given word. Emits <word, total count> pairs.
 */
public class Q2Reducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int total = 0;
        int males = 0;
        int females = 0;
        // calculate the total count
        for(Text val : values){
            String[] split = val.toString().split(" ");
            total += Integer.parseInt(split[2]);
            males += Integer.parseInt(split[0]);
            females += Integer.parseInt(split[1]);
        }
        context.write(new Text(key.toString() + " Males Never Married: "),new Text(""+ ((double)males/total)*100   ));
        context.write(new Text(key.toString() + " Females Never Married: "),new Text(""+((double)females/total)*100   ));

    }
}
