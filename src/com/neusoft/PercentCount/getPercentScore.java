package com.neusoft.PercentCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class getPercentScore {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFs", "file:///");

        Job job = Job.getInstance(conf);

        job.setJarByClass(getPercentScore.class);
        job.setMapperClass(getPercentScoreMap.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(0);

        Path path = new Path("E:\\IdeaProjects\\hadoopTest\\output1\\PercentScore");
        FileSystem fs = FileSystem.get(conf);

        if(fs.exists(path))
        {
            fs.delete(path, true);
        }

        FileInputFormat.setInputPaths(job, "E:\\IdeaProjects\\hadoopTest\\output1\\MoviesPercentScore");
        FileOutputFormat.setOutputPath(job, path);

        boolean b = job.waitForCompletion(true);

        System.exit(b?0:1);

    }

    static class getPercentScoreMap extends Mapper<LongWritable, Text, Text, Text>{

        Text k = new Text();
        Text v= new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            int total = 1291;
            String[] split = value.toString().split("\t");
            k.set(split[0]);
            double result = (double)Integer.parseInt(split[1])/total;
            String str = ""+result;
            v.set(str.substring(0, str.indexOf('.')+4));
            context.write(k, v);
        }
    }

}
