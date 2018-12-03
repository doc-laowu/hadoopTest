package com.neusoft.WordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    Text k = new Text();
    IntWritable v = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //1.首先获取每行的数据
        String str = value.toString();
        //2.切割获得数据
        String[] split = str.split("\t");
        //3.封装输出的数据
        for (String o: split)
        {
            k.set(o);
            context.write(k, v);
        }

    }
}
