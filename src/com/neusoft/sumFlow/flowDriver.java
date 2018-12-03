package com.neusoft.sumFlow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class flowDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        //是否运行为本地模式主要是看这个参数是否为local,默认为local
        conf.set("mapreduce.framework.name", "local");

        //运行在集群上面, 必须设置以下两个参数
        //conf.set("mapreduce.framework.name", "yarn");
        //conf.set("yarn.resourcemanager.hostname", "node01");

        //设置从本地读取输入数据
        conf.set("fs.defaultFs", "file:///");

        //设置从hdfs上读取输入数据
        //conf.set("fs.defaultFs", "hdfs://node01:9000");

        Job job = Job.getInstance(conf);

        job.setJarByClass(flowDriver.class);

        job.setMapperClass(flowMapper.class);
        job.setReducerClass(flowReducer.class);


        //指定我们的数据分区器
        job.setPartitionerClass(ProvincePartitioner.class);
        //指定我们的相应的数据分区数量的reducerTask任务
        job.setNumReduceTasks(5);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(flowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(flowBean.class);

//        FileInputFormat.setInputPaths(job,
//                new Path("E:\\IdeaProjects\\hadoopTest\\src\\flow.log"));

        //当小文件比较多的时候，会合并这些小文件然后，再maptask任务。
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setInputPaths(job,
                  new Path("E:\\IdeaProjects\\hadoopTest\\src\\flow.log"));
        CombineTextInputFormat.setMaxInputSplitSize(job, 4*1024*1024);
        CombineTextInputFormat.setMinInputSplitSize(job, 2*1024*1024);

        FileOutputFormat.setOutputPath(job,
                new Path("E:\\IdeaProjects\\hadoopTest\\src\\output2"));

//        MultipleOutputs.addNamedOutput(job, "data",
//                FileOutputFormat.class, Text.class, flowBean.class);

        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);

    }

}
