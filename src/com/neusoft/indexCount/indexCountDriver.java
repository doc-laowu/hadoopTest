package com.neusoft.indexCount;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class indexCountDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, indexCountDriver.class.getSimpleName()+"1");
        Job job2 = Job.getInstance(conf, indexCountDriver.class.getSimpleName()+"2");

        //配置第一个job任务
        job1.setJarByClass(indexCountDriver.class);

        job1.setMapperClass(indexMapper.class);

        job1.setReducerClass(indexReducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job1,new Path("E:\\IdeaProjects\\hadoopTest\\SrcInput\\indexInput"));
        FileOutputFormat.setOutputPath(job1,new Path("E:\\IdeaProjects\\hadoopTest\\SrcInput\\indexOutPut1"));

        //配置第二个job任务
        job2.setJarByClass(indexCountDriver.class);

        job2.setMapperClass(IndexMapper_two.class);

        job2.setReducerClass(indexReducer_two.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job2,new Path("E:\\IdeaProjects\\hadoopTest\\SrcInput\\indexOutPut1"));
        FileOutputFormat.setOutputPath(job2,new Path("E:\\IdeaProjects\\hadoopTest\\SrcInput\\indexOutPut2"));


        // 创建受控作业
        ControlledJob cjob1 = new ControlledJob(conf);
        ControlledJob cjob2 = new ControlledJob(conf);

        // 将普通作业包装成受控作业
        cjob1.setJob(job1);
        cjob2.setJob(job2);

        // 设置依赖关系
        cjob2.addDependingJob(cjob1);

        // 新建作业控制器
        JobControl jc = new JobControl("My control job");

        // 将受控作业添加到控制器中
        jc.addJob(cjob1);
        jc.addJob(cjob2);

        //启动job
        Thread jcThread = new Thread(jc);
        jcThread.start();
        while(true){
            if(jc.allFinished()){
                System.out.println(jc.getSuccessfulJobList());
                jc.stop();
                return ;
            }
            if(jc.getFailedJobList().size() > 0){
                System.out.println(jc.getFailedJobList());
                jc.stop();
                return ;
            }
        }
    }

}
