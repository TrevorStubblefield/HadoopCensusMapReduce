package cs455.hadoop.Q9;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives word, list<count> pairs.
 * Sums up individual counts per given word. Emits <word, total count> pairs.
 */
public class Q9Combiner extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        int ownedTenants = 0;
        int ownedHouses = 0;
        int rentedTenants = 0;
        int rentedHouses = 0;

        for (Text val : values){
            String text = val.toString();
            String[] split = text.split(" ");
            ownedTenants += Integer.parseInt(split[0]);
            ownedHouses += Integer.parseInt(split[1]);
            rentedTenants += Integer.parseInt(split[2]);
            rentedHouses += Integer.parseInt(split[3]);
        }

        context.write(new Text(""),new Text(key.toString() + " " + ownedTenants + " " + ownedHouses + " " + rentedTenants + " " + rentedHouses));


    }
}
