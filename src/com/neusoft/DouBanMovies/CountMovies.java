package com.neusoft.DouBanMovies;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CountMovies {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFs", "file:///");

        Job job = Job.getInstance(conf);

        job.setJarByClass(CountMovies.class);
        job.setMapperClass(CountMap.class);
        job.setReducerClass(CountReduce.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.setCombinerClass(CountReduce.class);

        job.setNumReduceTasks(1);

        Path path = new Path("E:\\IdeaProjects\\hadoopTest\\output\\count");
        FileSystem fs = FileSystem.get(conf);

        if(fs.exists(path))
        {
            fs.delete(path, true);
        }

        FileInputFormat.setInputPaths(job, "E:\\IdeaProjects\\hadoopTest\\SrcInput\\DoubanInput");
        FileOutputFormat.setOutputPath(job, path);

        boolean b = job.waitForCompletion(true);

        System.exit(b?0:1);

    }

    static class CountMap extends Mapper<LongWritable, Text, NullWritable, IntWritable>{

        IntWritable v = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            context.write(NullWritable.get(), v);

        }
    }

    static class CountReduce extends Reducer<NullWritable, IntWritable, NullWritable, IntWritable>{

        IntWritable v = new IntWritable();
        @Override
        protected void reduce(NullWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int total = 0;
            for(IntWritable o : values){
                total += o.get();
            }
            v.set(total);
            context.write(NullWritable.get(), v);
        }
    }

}
