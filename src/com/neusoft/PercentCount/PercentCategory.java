package com.neusoft.PercentCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PercentCategory {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFs", "file:///");

        Job job = Job.getInstance(conf);

        job.setJarByClass(PercentCategory.class);
        job.setMapperClass(PercentCategoryMap.class);
        job.setReducerClass(PercentCategoryReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(1);

        job.setCombinerClass(PercentCategoryReduce.class);

        Path path = new Path("E:\\IdeaProjects\\hadoopTest\\output1\\MoviesLanguage");
        FileSystem fs = FileSystem.get(conf);

        if(fs.exists(path))
        {
            fs.delete(path, true);
        }

        FileInputFormat.setInputPaths(job, "E:\\IdeaProjects\\hadoopTest\\output\\MoviesLanguage");
        FileOutputFormat.setOutputPath(job, path);

        boolean b = job.waitForCompletion(true);

        System.exit(b?0:1);

    }

    static class PercentCategoryMap extends Mapper<LongWritable, Text, Text, IntWritable> {

        Text k = new Text();
        IntWritable v = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] s = value.toString().split("\t");
            int nums = Integer.parseInt(s[1]);
            if(nums > 10){
                k.set(s[0]);
            }else{
                k.set("其他");
            }
            v.set(nums);
            context.write(k, v);
        }
    }

    static class PercentCategoryReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        IntWritable v = new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int total = 0;
            for(IntWritable o : values){
                total += o.get();
            }
            v.set(total);
            context.write(key, v);
        }
    }

}
