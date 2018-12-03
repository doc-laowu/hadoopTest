package com.neusoft.indexCount;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class indexMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    String name = null;

    Text k = new Text();

    IntWritable v = new IntWritable(1);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        name = inputSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] split = value.toString().split(" ");
        for(String str : split)
        {
            k.set(str+"--"+name);
            context.write(k, v);
        }
    }
}
