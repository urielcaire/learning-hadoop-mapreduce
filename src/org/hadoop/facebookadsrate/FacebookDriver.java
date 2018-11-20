package org.hadoop.facebookadsrate;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/***
 * A java project to analyse some Facebook Ads data
 */
public class FacebookDriver
{
    public static void main(String[] args) throws IOException,	ClassNotFoundException, InterruptedException
    {

        Path input_path = new Path("hdfs://localhost:9000/user/{user}/fb");
        Path output_dir = new Path("hdfs://localhost:9000/user/{user}/output");

        Configuration conf = new Configuration();
        Job job = new Job(conf, "Facebook analysis");

        // name of driver class
        job.setJarByClass(FacebookDriver.class);
        // name of mapper class
        job.setMapperClass(FacebookMapper.class);
        // name of reducer class
        job.setReducerClass(FacebookReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, input_path);
        FileOutputFormat.setOutputPath(job, output_dir);
        output_dir.getFileSystem(job.getConfiguration()).delete(output_dir,	true);

        job.waitForCompletion(true);
    }
}
