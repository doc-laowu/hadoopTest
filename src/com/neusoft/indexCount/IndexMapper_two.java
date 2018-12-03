package com.neusoft.indexCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class IndexMapper_two extends Mapper<LongWritable, Text, Text, Text> {

    enum myCounter{MALFORORMED,NORMAL};

    Text k = new Text();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] split = line.split("--");
        k.set(split[0]);
        v.set(split[1]);
        context.write(k,v);

        //1）采用枚举的方式统计计数
        context.getCounter(myCounter.NORMAL).increment(1);
        //2）采用计数器组、计数器名称的方式统
        context.getCounter("indexMapper", "Normal").increment(1);
    }
}
