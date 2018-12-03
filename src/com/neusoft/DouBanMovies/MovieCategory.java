package com.neusoft.DouBanMovies;

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

public class MovieCategory {

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFs", "file:///");

        Job job = Job.getInstance(conf);

        job.setJarByClass(MovieCategory.class);
        job.setMapperClass(MovieCategoryMapper.class);
        job.setReducerClass(MovieCategoryReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        Path path = new Path("E:\\IdeaProjects\\hadoopTest\\output\\category");
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

    static class MovieCategoryMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

        Text k = new Text();
        IntWritable v = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] split = value.toString().split("\t")[3].split("-");
            for(String o : split) {
                k.set(o);
                context.write(k, v);
            }
        }
    }

    static class MovieCategoryReduce extends Reducer<Text, IntWritable, Text, IntWritable>{

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
