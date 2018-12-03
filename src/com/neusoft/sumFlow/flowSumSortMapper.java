package com.neusoft.sumFlow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class flowSumSortMapper {

    static class flowSumSortMappers extends Mapper<LongWritable, Text, flowBean, Text>
    {

        flowBean fb = new flowBean();

        Text v = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            String[] split = line.split("\t");
            v.set(split[1]);
            fb.set(Long.parseLong(split[split.length-3]), Long.parseLong(split[split.length-2]));
            context.write(fb, v);

        }
    }

    static class flowSumSortReducer extends Reducer<flowBean, Text, Text, flowBean>
    {
        @Override
        protected void reduce(flowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //由于每个对象都是唯一的
            context.write(values.iterator().next(), key);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFs", "file:///");
        Job job = Job.getInstance(conf);

        job.setJarByClass(flowSumSortMapper.class);

        job.setMapperClass(flowSumSortMappers.class);
        job.setReducerClass(flowSumSortReducer.class);

        job.setMapOutputKeyClass(flowBean.class);
        job.setMapOutputValueClass(Text.class);


        //指定我们的数据分区器
        job.setPartitionerClass(ProvincePartitioner.class);
        //指定我们的相应的数据分区数量的reducerTask任务
        job.setNumReduceTasks(5);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(flowBean.class);

        Path inputPath = new Path("E:\\IdeaProjects\\hadoopTest\\src\\flow.log");
        Path outputPath = new Path("E:\\IdeaProjects\\hadoopTest\\src\\output2");

        FileSystem fileSystem = FileSystem.get(conf);

        if(!fileSystem.exists(inputPath))
            System.exit(1);

        if(fileSystem.exists(outputPath))
            fileSystem.delete(outputPath, true);

        //如果不设置fileinputformat的话，默认使用的是TextInputFormat类，
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }

}
