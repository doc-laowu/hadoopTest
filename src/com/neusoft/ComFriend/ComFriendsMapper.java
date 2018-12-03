package com.neusoft.ComFriend;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class ComFriendsMapper extends Mapper<LongWritable, Text, Text, Text> {

    Text k = new Text();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //转换为字符串
        String line = value.toString();

        //字符串切割
        String[] split1 = line.split("\t");
        String[] split2 = split1[1].split(" ");
        Arrays.sort(split2);
        //构建输出键值对
        for(int i=0; i<split2.length; i++)
        {
            for(int j=i+1; j<split2.length; j++)
            {
                k.set(split2[i]+"-"+split2[j]);
                v.set(split1[0]);
                context.write(k, v);
            }
        }
    }
}
