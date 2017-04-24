package cs455.hadoop.Q8;

import cs455.hadoop.Record;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * On a per-state basis provide a breakdown of the percentage of residences that were rented vs. owned.
 */
public class Q8Mapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        Record record = new Record(value.toString());

        //String values = record.state + " " + record.getNumberOfElderly();
        String values = record.getNumberOfElderly();

        if(!record.getNumberOfElderly().equals("")) {
            context.write(new Text(record.state), new Text(values));
        }

    }
}
