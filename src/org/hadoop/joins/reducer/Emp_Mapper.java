package org.hadoop.joins.reducer;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper of employees info
 */
public class Emp_Mapper extends Mapper<LongWritable, Text, Text, Text>
{

    // key=0  value=1,Jack

    public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException
    {

        String line = value.toString();         //  line =  1, Jack

        String[] EmployeeInfo = line.split(",");       // EmployeeInfo = [ {1} {Jack} ]

        context.write(new Text(EmployeeInfo[0]), new Text("Emp,"+EmployeeInfo[1]));
        // 1                           Emp,Jack
    }
}

