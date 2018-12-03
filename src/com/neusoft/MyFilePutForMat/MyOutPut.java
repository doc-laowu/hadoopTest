package com.neusoft.MyFilePutForMat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyOutPut {

    public static class myOutPutMapper extends Mapper<LongWritable, Text, Text, NullWritable>
    {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            context.write(value, NullWritable.get());

        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        Job job = Job.getInstance(conf);

        job.setJarByClass(MyOutPut.class);

        job.setMapperClass(myOutPutMapper.class);

        //job.setGroupingComparatorClass(myGroupingComparator.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(FilterOutPutFormat.class);

        FileInputFormat.setInputPaths(job, new Path("E:\\IdeaProjects\\hadoopTest\\SrcInput\\UrlsInput"));

        Path outpath = new Path("E:\\IdeaProjects\\hadoopTest\\SrcInput\\Urls_Output");

        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(outpath)) {
            fs.delete(outpath, true);
        }

        FileOutputFormat.setOutputPath(job, outpath);

        job.setNumReduceTasks(0);

        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);


        KeyValueTextInputFormat

    }

}
