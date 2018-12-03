package com.neusoft.ComFriend;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ComFriendsReducer extends Reducer<Text, Text, Text, Text> {

    Text v = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuilder sb = new StringBuilder();
        for(Text o : values)
        {
            sb.append(o.toString()+" ");
        }
        v.set(sb.toString());
        context.write(key, v);
    }
}
