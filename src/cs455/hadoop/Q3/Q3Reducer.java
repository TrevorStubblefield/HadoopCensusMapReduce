package cs455.hadoop.Q3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives word, list<count> pairs.
 * Sums up individual counts per given word. Emits <word, total count> pairs.
 */
public class Q3Reducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        int male1 = 0;
        int male2 = 0;
        int male3 = 0;

        int female1 = 0;
        int female2 = 0;
        int female3 = 0;

        int total = 0;

        // calculate the total count
        for(Text val : values){
            String[] data = val.toString().split(" ");
            male1 += Integer.parseInt(data[0]);
            male2 += Integer.parseInt(data[1]);
            male3 += Integer.parseInt(data[2]);
            female1 += Integer.parseInt(data[3]);
            female2 += Integer.parseInt(data[4]);
            female3 += Integer.parseInt(data[5]);
            total += Integer.parseInt(data[6]);

        }
        context.write(new Text(key.toString() + " Males Under 18: "), new Text(""+ ((double)male1/total)*100 + "%"));
        context.write(new Text(key.toString() + " Males Between 19 and 29: "), new Text(""+ ((double)male2/total)*100 + "%"));
        context.write(new Text(key.toString() + " Males Between 30 and 39: "), new Text(""+((double)male3/total)*100 + "%"));
        context.write(new Text(key.toString() + " Females Under 18: "), new Text(""+((double)female1/total)*100 + "%"));
        context.write(new Text(key.toString() + " Females Between 19 and 29: "), new Text(""+((double)female2/total)*100 + "%"));
        context.write(new Text(key.toString() + " Females Between 30 and 39: "), new Text(""+((double)female3/total)*100 + "%"));

    }
}
