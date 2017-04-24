package cs455.hadoop.Q7;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;
import java.util.stream.*;
import java.util.Map.*;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives word, list<count> pairs.
 * Sums up individual counts per given word. Emits <word, total count> pairs.
 */
public class Q7Reducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        LinkedHashMap<String, Integer> map = new LinkedHashMap<>();
        LinkedHashMap<String, Integer> houseMap = new LinkedHashMap<>();

        // Gets the total rooms and places them in map based on state.
        // Getts the total houses and places them in houseMap based on state.
        for(Text val : values){
            String text = val.toString();
            String[] split = text.split(" ");
            String state = split[0];
            Integer rooms = new Integer(split[1]);
            Integer houses = new Integer(split[2]);

            if (!map.containsKey(state)){
                map.put(state, rooms);
            }
            else{
                Integer newRooms = map.get(state) + rooms;
                map.replace(state, newRooms);
            }

            if (!houseMap.containsKey(state)){
                houseMap.put(state, houses);
            }
            else{
                Integer newHouses = houseMap.get(state) + houses;
                houseMap.replace(state, newHouses);
            }
        }

        List<String> keySet = new ArrayList<>(map.keySet());

        double percentile;
        double averageSum = 0;

        LinkedHashMap<String, Double> averagedMap = new LinkedHashMap<>();

        for(int i = 0; i < keySet.size(); i++){
            String state = keySet.get(i);
            double rooms = map.get(state);
            double houses = houseMap.get(state);
            averageSum += (rooms/houses);
            averagedMap.put(state, (rooms/houses));
        }
        percentile = 0.95 * averageSum;

        //Sorts the averaged map by value.
        Set<Entry<String,Double>> set = averagedMap.entrySet();
        Stream<Entry<String,Double>> stream = set.stream();
        Stream<Entry<String,Double>> sortedStream  = stream.sorted(Entry.comparingByValue());
        LinkedHashMap<String, Double>sortedMap = sortedStream.collect(Collectors.toMap(Entry::getKey, Entry::getValue, (entry1, entry2) -> entry1, LinkedHashMap::new));

        List<String> sortedKeySet = new ArrayList<>(sortedMap.keySet());
        double sum = 0;

        for(int i = 0; i < sortedKeySet.size(); i++) {
            String state = sortedKeySet.get(i);
            double value = sortedMap.get(state);
            sum += value;
            if(sum >= percentile){
                context.write(new Text("95th percentile of the average number of rooms per house across all states: "), new Text(""+averagedMap.get(state)));
                break;
            }
        }



    }
}
