package com.neusoft.PercentCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

/*
问题：现在有一个很大的数据，假设有几千万条但不知道具体有多少条，如何在只遍历一次的情况下，
随机取出其中K条数据？<p></p><p>    思路：可以将此问题抽象为蓄水池抽样问题。即，先把读取到的前K条数据放入列表中，
对于第K+1个对象，以K/(K+1)的概率选择该对象；对于第K+2个对象，以K/(K+2)的概率选择该对象；以此类推，以K/M的概率选择第M个对象(M>K)。
如果M被选中,则随机替换列表中的一个对象。如果数据总量N无穷大，则每个对象被选中的概率将均为K/N。</p><p>    </p><p>   
设计Mapper：</p><p>    首先要在setup中初始化K的值，也就是随机抽样的个数，然后在map中记录此刻传进来的值在数据流中的位置row，
如果row小于K，就将此条数据放入列表中；如果row大于K，则随机生成一个0到row之间的数m，如果m小于K，则将此条数据替换列表中第m条数据，
否则不替换。</p><p>   当所有数据经过map后就得到了一个大小为K的列表，这个列表就是我们随机得到的数据。如果数据量小于一个split的大小，
则可以省略Reduce过程，直接在cleanup中输出到HDFS。
 */


public class SocreVotes {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFs", "file:///");

        Job job = Job.getInstance(conf);

        job.setJarByClass(SocreVotes.class);
        job.setMapperClass(SocreVotesMap.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(0);

        Path path = new Path("E:\\IdeaProjects\\hadoopTest\\output1\\ScoreVotes");
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

    static class SocreVotesMap extends Mapper<LongWritable, Text, Text, NullWritable>{

        private int row = 0;
        private int k=0;
        private ArrayList<Text> result = new ArrayList<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            k = 100;
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            row++;

            String[] split = value.toString().split("\t");

            if(row <= k){
                result.add(new Text(split[7]+"\t"+split[8]));
            }
            else{
                int p = randI(row);
                if(p < k){
                    result.set(p, new Text(split[7]+"\t"+split[8]));
                }
            }
        }

        Random random = new Random();
        private int randI(int max) {
            return random.nextInt(max);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for(int i=0;i<result.size();i++)
                context.write(result.get(i),NullWritable.get());
        }
    }

}
