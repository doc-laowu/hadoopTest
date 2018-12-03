package com.neusoft.ComFriend;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ComUsersMapper extends Mapper<LongWritable, Text, Text, Text> {

    Text k = new Text();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //转换为字符串
        String line = value.toString();

        //切割字符串
        String[] split1 = line.split(":");
        String[] split2 = split1[1].split(",");

        //得到每个好友的公共用户
        for(String o : split2)
        {
            k.set(o);
            v.set(split1[0]);
            //写出成键值对
            context.write(k, v);
        }
    }
}
