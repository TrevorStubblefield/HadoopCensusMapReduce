package cs455.hadoop.Q5;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives word, list<count> pairs.
 * Sums up individual counts per given word. Emits <word, total count> pairs.
 */
public class Q5Reducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        //Creates a map for each of the categories.
        LinkedHashMap<String, Integer> map = new LinkedHashMap<>();
        map.put("lessThan$15000",0);
        map.put("$15000to$19999",0);
        map.put("$20000to$24999",0);
        map.put("$25000to$29999",0);
        map.put("$30000to$34999",0);
        map.put("$35000to$39999",0);
        map.put("$40000to$44999",0);
        map.put("$45000to$49999",0);
        map.put("$50000to$59999",0);
        map.put("$60000to$74999",0);
        map.put("$75000to$99999",0);
        map.put("$100000to$124999",0);
        map.put("$125000to$149999",0);
        map.put("$150000to$174999",0);
        map.put("$175000to$199999",0);
        map.put("$200000to$249999",0);
        map.put("$250000to$299999",0);
        map.put("$300000to$399999",0);
        map.put("$400000to499999",0);
        map.put("$500000OrMore",0);

        int total = 0;

        //Gets each value and updates the map based on price bracket.
        for(Text val : values){
            String text = val.toString();
            String[] split = text.split(" ");
            String bracket = split[0];
            Integer value = new Integer(split[1]);
            total += value;

            int old = map.get(bracket);
            map.replace(bracket, old+value);
        }

        //Calculates the median, based on half way of the total.
        int medianValue = (total+1)/2;

        List<String> keySet = new ArrayList<>(map.keySet());
        int totalIterated = 0;

        for (int i = 0; i < keySet.size(); i++){
            String k = keySet.get(i);
            Integer v = map.get(k);
            if (medianValue >= totalIterated && medianValue < (totalIterated+v))
                context.write(new Text(key.toString() + " Median House Value: "),new Text(k));
            totalIterated+=v;
        }

    }
}
