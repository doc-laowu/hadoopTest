package com.neusoft.WordCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class WordCountDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //获取job的信息
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");

        Job instance = Job.getInstance(conf);

        //设置jar包的路径
        instance.setJarByClass(WordCountDriver.class);

        //设置mapper和reducer类
        instance.setMapperClass(WordCountMapper.class);
        instance.setReducerClass(WordCountReducer.class);

        //获取map输出类型
        instance.setMapOutputKeyClass(Text.class);
        instance.setMapOutputValueClass(IntWritable.class);

        //设置最后的输出类型
        instance.setOutputKeyClass(Text.class);
        instance.setOutputValueClass(IntWritable.class);

        //设置文件路径
        FileInputFormat.setInputPaths(instance,
                new Path("E:\\IdeaProjects\\hadoopTest\\src\\wordcountFile.txt")); //设置要读取的文件路径
        FileOutputFormat.setOutputPath(instance,
                new Path("E:\\IdeaProjects\\hadoopTest\\src\\output")); //设置输出结果的文件路径

//        FileInputFormat.setInputPaths(instance,
//                args[0]); //设置要读取的文件路径
//
//        FileOutputFormat.setOutputPath(instance,
//                new Path(args[1])); //设置输出结果的文件路径

        //执行并退出
        boolean b = instance.waitForCompletion(true);
        System.exit(b?0:1);

    }

}
