package org.hadoop.joins.mapper;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.filecache.DistributedCache;
import java.net.URI;
import java.net.URISyntaxException;

public class MapJoin
{
    public static void main(String[] args) throws IOException,  ClassNotFoundException, InterruptedException,URISyntaxException
    {
        Path inputPath = new Path("hdfs://localhost:9000/user/{user}/sales.txt");
        Path outputDir = new Path("hdfs://localhost:9000/user/{user}/mapjoin/");

        Configuration conf = new Configuration();

        // add 2 files to cache
        DistributedCache.addCacheFile(new URI("hdfs://localhost:9000/user/{user}/store.txt"), conf);
        DistributedCache.addCacheFile(new URI("hdfs://localhost:9000/user/{user}/product.txt"), conf);
        // JOb name
        Job job = new Job(conf, "MapJoin");

        //name of driver class
        job.setJarByClass(MapJoin.class);
        //name of Mapper class
        job.setMapperClass(MapJoinMapper.class);
        //name of Reducer class
        job.setReducerClass(MapJoinReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputDir);

        outputDir.getFileSystem(job.getConfiguration()).delete(outputDir,true);
        job.waitForCompletion(true);
    }}
