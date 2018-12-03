package com.neusoft.ComFriend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ComFriendDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        conf.set("fs.defalutFs", "file:///");

//        Job job = Job.getInstance(conf);
//
//        job.setJarByClass(ComFriendDriver.class);
//
//        job.setMapperClass(ComUsersMapper.class);
//        job.setReducerClass(ComUsersReducer.class);
//
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(Text.class);
//
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
//
//        FileInputFormat.setInputPaths(job,
//                new Path("E:\\IdeaProjects\\hadoopTest\\src\\data.log"));
//        FileOutputFormat.setOutputPath(job,
//                new Path("E:\\IdeaProjects\\hadoopTest\\src\\output"));
//
//        boolean b = job.waitForCompletion(true);
//        System.exit(b?0:1);


        Job job1 = Job.getInstance(conf);

        job1.setJarByClass(ComFriendDriver.class);

        job1.setMapperClass(ComFriendsMapper.class);
        job1.setReducerClass(ComFriendsReducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job1,
                new Path("E:\\IdeaProjects\\hadoopTest\\src\\output\\part-r-00000"));
        FileOutputFormat.setOutputPath(job1,
                new Path("E:\\IdeaProjects\\hadoopTest\\src\\output2"));

        boolean b1 = job1.waitForCompletion(true);
        System.exit(b1?0:1);

    }

}
