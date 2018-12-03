package com.neusoft.WordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        //遍历结果集
        int sum = 0;
        for(IntWritable o : values)
        {
            sum += o.get();
        }
        //组装最后的结果
        context.write(key, new IntWritable(sum));
    }
}
