package com.neusoft.orderBean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class OederBeanDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {


        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        Job job = Job.getInstance(conf);

        job.setJarByClass(OederBeanDriver.class);

        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderReducer.class);

        //job.setGroupingComparatorClass(myGroupingComparator.class);

        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("E:\\IdeaProjects\\hadoopTest\\src\\groupingComparator.txt"));

        Path outpath = new Path("E:\\IdeaProjects\\hadoopTest\\src\\output1");

        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(outpath)) {
            fs.delete(outpath, true);
        }
        FileOutputFormat.setOutputPath(job, outpath);

        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);

    }

}
