package cs455.hadoop.Q2;

import cs455.hadoop.Record;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * On a per-state basis provide a breakdown of the percentage of residences that were rented vs. owned.
 */
public class Q2Mapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        Record record = new Record(value.toString());

        String malesNeverMarried = record.getMaleNeverMarried();
        String femalesNeverMarried = record.getFemaleNeverMarried();
        String totalPopulation = record.getTotalPopulation();

        if(!malesNeverMarried.equals("") && !femalesNeverMarried.equals("")) {
            context.write(new Text(record.state), new Text(malesNeverMarried + " " + femalesNeverMarried + " " + totalPopulation));
        }

    }
}
