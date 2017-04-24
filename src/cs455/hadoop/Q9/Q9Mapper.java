package cs455.hadoop.Q9;

import cs455.hadoop.Record;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * On a per-state basis provide a breakdown of the percentage of residences that were rented vs. owned.
 */
public class Q9Mapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        Record record = new Record(value.toString());

        String owned = record.getNumberOfTenantsOwned();
        String rented = record.getNumberOfTenantsRented();

        if(!owned.equals("") && !rented.equals("")) {
            context.write(new Text(record.state), new Text(owned + " " + rented));
        }

    }
}
