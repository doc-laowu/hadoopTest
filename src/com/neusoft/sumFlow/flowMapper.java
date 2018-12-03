package com.neusoft.sumFlow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class flowMapper extends Mapper<LongWritable, Text, Text, flowBean> {

    Text k = new Text();
    flowBean fb = new flowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split("\t");
        k.set(split[1]);
        fb.set(Long.parseLong(split[split.length-3]), Long.parseLong(split[split.length-2]));
        context.write(k, fb);
    }
}
