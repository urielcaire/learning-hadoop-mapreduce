package org.hadoop.joins.reducer;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper of locations
 */
public class Location_Mapper extends Mapper<LongWritable, Text, Text, Text>
{
    //key = 0  value  5,London
    public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException
    {

        String line = value.toString().trim();    // 5,London

        String[] AddressInfo = line.split(",");    // [{5} {London} ]

        context.write(new Text(AddressInfo[0]), new Text("Address,"+AddressInfo[1]));
    }             // 5                               Address, London
}
