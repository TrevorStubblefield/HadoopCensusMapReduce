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
public class Q8Reducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {


        HashMap<String, Integer> totalMap = new HashMap<>();
        HashMap<String, Integer> elderlyMap = new HashMap<>();

        for (Text val : values) {
            String text = val.toString();
            String[] split = text.split(" ");
            String state = split[0];
            String total = split[1];
            String elderly = split[2];

            if(!totalMap.containsKey(state)) {
                totalMap.put(state,new Integer(total));
            }
            else{
                int v = totalMap.get(state) + new Integer(total);
                totalMap.replace(state, v);
            }

            if(!elderlyMap.containsKey(state)) {
                elderlyMap.put(state,new Integer(elderly));
            }
            else{
                int v = elderlyMap.get(state) + new Integer(elderly);
                elderlyMap.replace(state, v);
            }
        }

        List<String> keySet = new ArrayList<>(totalMap.keySet());

        double highestPercentage = 0;
        String highestState = "";

        for(int i = 0; i < keySet.size(); i++){
            String state = keySet.get(i);
            double total = totalMap.get(state);
            double elderly = elderlyMap.get(state);
            double percentage = elderly/total;
            if(percentage>highestPercentage) {
                highestPercentage = percentage;
                highestState = state;
            }
        }
        context.write(new Text("State With Highest Percentage Of Elderly: "), new Text(highestState + "   " + (highestPercentage*100) + "%"));

    }
}
