package com.neusoft.rJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

/**
 * 解决数据倾斜的方法
 */


public class MapSideJoin {

    static class MapSideJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable>
    {

        Map<String, String> product = new HashMap<>();

        Text k = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            BufferedReader br =
                    new BufferedReader(new InputStreamReader(new FileInputStream("E:/IdeaProjects/hadoopTest/SrcInput/pdts.txt")));

            String line = null;
            while((line = br.readLine()) != null )
            {
                String[] split = line.split(",");
                product.put(split[0], split[1]);
            }

            br.close();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String order_line = value.toString();
            String[] split = order_line.split("\t");
            String s = product.get(split[1]);

            k.set(order_line+"\t"+s);

            context.write(k, NullWritable.get());

        }
    }


    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFs", "file:///");

        Job job = Job.getInstance(conf);

        job.setJarByClass(MapSideJoin.class);

        job.setMapperClass(MapSideJoinMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);


        FileInputFormat.setInputPaths(job,
                "E:\\IdeaProjects\\hadoopTest\\SrcInput\\MapJoin\\orders.txt");
        FileOutputFormat.setOutputPath(job, new Path("E:\\IdeaProjects\\hadoopTest\\SrcInput\\MapJoinOutput"));

//        缓存jar包到task运行节点的classpath种
//        job.addArchiveToClassPath();

//        缓存压缩包文件到task运行的工作目录
//        job.addCacheArchive();

//        缓存普通文件到task运行的工作目录
        job.addCacheFile(new URI("file:/E:/IdeaProjects/hadoopTest/SrcInput/pdts.txt"));

//        缓存普通文件到task运行节点的classpath种
//        job.addFileToClassPath();

        job.setNumReduceTasks(0);

        boolean b = job.waitForCompletion(true);

        System.exit(b?0:1);

    }

}
