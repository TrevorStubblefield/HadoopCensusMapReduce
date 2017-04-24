package cs455.hadoop.Q6;

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
public class Q6Reducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
        LinkedHashMap<String, Integer> map = new LinkedHashMap<>();

        map.put("lessThan$100",0);
        map.put("$100to$149",0);
        map.put("$150to$199",0);
        map.put("$200to$249",0);
        map.put("$250to$299",0);
        map.put("$300to$349",0);
        map.put("$350to$399",0);
        map.put("$400to$449",0);
        map.put("$450to$499",0);
        map.put("$500to$549",0);
        map.put("$550to$599",0);
        map.put("$600to$649",0);
        map.put("$650to$699",0);
        map.put("$700to$749",0);
        map.put("$750to$999",0);
        map.put("$1000OrMore",0);

        int total = 0;

        for(Text val : values){
            String text = val.toString();
            String[] split = text.split(" ");
            String bracket = split[0];
            Integer value = new Integer(split[1]);
            total += value;

            int old = map.get(bracket);
            map.replace(bracket, old+value);
        }

        int medianValue = (total+1)/2;

        List<String> keySet = new ArrayList<>(map.keySet());
        int totalIterated = 0;

        for (int i = 0; i < keySet.size(); i++){
            String k = keySet.get(i);
            Integer v = map.get(k);
            if (medianValue >= totalIterated && medianValue < (totalIterated+v))
                context.write(new Text(key.toString() + " Median Rent Value: "),new Text(k));
            totalIterated+=v;
        }

    }
}
