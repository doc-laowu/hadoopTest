package com.neusoft.indexCount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class indexReducer_two extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuilder sb = new StringBuilder();
        for(Text v:values){
            sb.append(v.toString().replace("\t","-->")+"\t");
        }
        context.write(key,new Text(sb.toString()));

    }
}
