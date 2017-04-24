package cs455.hadoop.Q9;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives word, list<count> pairs.
 * Sums up individual counts per given word. Emits <word, total count> pairs.
 */
public class Q9Reducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        LinkedHashMap<String, Integer> ownedTenantsMap = new LinkedHashMap<>();
        LinkedHashMap<String, Integer> ownedHousesMap = new LinkedHashMap<>();
        LinkedHashMap<String, Integer> rentedTenantsMap = new LinkedHashMap<>();
        LinkedHashMap<String, Integer> rentedHousesMap = new LinkedHashMap<>();

        //Updates maps for both owned tenants/houses and rented tenants/houses.
        for(Text val : values){
            String text = val.toString();
            String[] split = text.split(" ");
            String state = split[0];
            Integer ownedTenants = new Integer(split[1]);
            Integer ownedHouses = new Integer(split[2]);
            Integer rentedTenants = new Integer(split[3]);
            Integer rentedHouses = new Integer(split[4]);

            //OWNED TENANTS
            if (!ownedTenantsMap.containsKey(state)){
                ownedTenantsMap.put(state, ownedTenants);
            }
            else{
                Integer newOT = ownedTenantsMap.get(state) + ownedTenants;
                ownedTenantsMap.replace(state, newOT);
            }


            //OWNED HOUSES
            if (!ownedHousesMap.containsKey(state)){
                ownedHousesMap.put(state, ownedHouses);
            }
            else{
                Integer newOH = ownedHousesMap.get(state) + ownedHouses;
                ownedHousesMap.replace(state, newOH);
            }


            //RENTED TENANTS
            if (!rentedTenantsMap.containsKey(state)){
                rentedTenantsMap.put(state, rentedTenants);
            }
            else{
                Integer newRT = rentedTenantsMap.get(state) + rentedTenants;
                rentedTenantsMap.replace(state, newRT);
            }


            //RENTED HOUSES
            if (!rentedHousesMap.containsKey(state)){
                rentedHousesMap.put(state, rentedHouses);
            }
            else{
                Integer newRH = rentedHousesMap.get(state) + rentedHouses;
                rentedHousesMap.replace(state, newRH);
            }
        }

        List<String> keySet = new ArrayList<>(ownedTenantsMap.keySet());

        LinkedHashMap<String, Double> averagedOwned = new LinkedHashMap<>();
        LinkedHashMap<String, Double> averagedRented = new LinkedHashMap<>();

        for (int i = 0; i < keySet.size(); i++){
            String state = keySet.get(i);
            double ownedTenant = ownedTenantsMap.get(state);
            double ownedHouse = ownedHousesMap.get(state);
            double rentedTenant = rentedTenantsMap.get(state);
            double rentedHouse = rentedHousesMap.get(state);
            averagedOwned.put(state,(ownedTenant/ownedHouse));
            averagedRented.put(state,(rentedTenant/rentedHouse));
        }

        //Sorts the averaged Owned map by value.
        Set<Entry<String,Double>> set = averagedOwned.entrySet();
        Stream<Entry<String,Double>> stream = set.stream();
        Stream<Entry<String,Double>> sortedStream  = stream.sorted(Entry.comparingByValue(Collections.reverseOrder()));
        LinkedHashMap<String, Double>sortedOwnedMap = sortedStream.collect(Collectors.toMap(Entry::getKey, Entry::getValue, (entry1, entry2) -> entry1, LinkedHashMap::new));

        //Sorts the averaged Rented map by value.
        set = averagedRented.entrySet();
        stream = set.stream();
        sortedStream  = stream.sorted(Entry.comparingByValue(Collections.reverseOrder()));
        LinkedHashMap<String, Double>sortedRentedMap = sortedStream.collect(Collectors.toMap(Entry::getKey, Entry::getValue, (entry1, entry2) -> entry1, LinkedHashMap::new));

        List<String> ownedKeySet = new ArrayList<>(sortedOwnedMap.keySet());
        List<String> rentedKeySet = new ArrayList<>(sortedRentedMap.keySet());

        for(int i = 0; i < 10; i++){
            String state = ownedKeySet.get(i);
            double value = averagedOwned.get(state);
            context.write(new Text("Highest Average Owned Tenant Count: "), new Text(state + " : " + value));
        }

        context.write(new Text(), new Text());

        for(int i = 0; i < 10; i++){
            String state = rentedKeySet.get(i);
            double value = averagedRented.get(state);
            context.write(new Text("Highest Average Rented Tenant Count: "), new Text(state + " : " + value));
        }




    }
}
