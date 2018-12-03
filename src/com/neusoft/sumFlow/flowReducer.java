package com.neusoft.sumFlow;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class flowReducer extends Reducer<Text, flowBean, Text, flowBean> {

//    private MultipleOutputs<Text, flowBean> multipleOutputs;

    @Override
    protected void reduce(Text key, Iterable<flowBean> values, Context context) throws IOException, InterruptedException {
        long upFlow = 0L;
        long downFlow = 0L;

        for(flowBean fb:values)
        {
            upFlow += fb.getUpFlow();
            downFlow += fb.getDownFlow();
        }

        context.write(key, new flowBean(upFlow, downFlow));

//        multipleOutputs.write(key.toString(), key, new flowBean(upFlow, downFlow));

    }

//    @Override
//    protected void setup(Context context) throws IOException, InterruptedException {
//        multipleOutputs = new MultipleOutputs<Text, flowBean>(context);
//        super.setup(context);
//    }
//
//    @Override
//    protected void cleanup(Context context) throws IOException, InterruptedException {
//        multipleOutputs.close();
//    }
}
