package com.neusoft.rJoin;


import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RJoin {

    static class RJoinMapper extends Mapper<LongWritable, Text, Text, InfoBean> {

        InfoBean fb = new InfoBean();
        Text k = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] split = value.toString().split("\t");

            String name = ((FileSplit) context.getInputSplit()).getPath().getName();

            String pid;

            //当flag为0表示为订单信息
            //当flag为1表示为商品信息

            if(name.startsWith("t_order"))
            {
                pid = new String(split[2].getBytes(),"GBK");
                fb.setOrder_id(split[0]);
                fb.setDateString(split[1]);
                fb.setAmount(Integer.parseInt(split[3]));

                fb.setP_id(split[2]);

                fb.setPname("");
                fb.setCategory_id("");
                fb.setPrice(0.0f);

                fb.setFlag("0");
            }
            else
            {
                pid = split[0];

                fb.setOrder_id("");
                fb.setDateString("");
                fb.setAmount(0);

                fb.setP_id(split[0]);
                fb.setPname(split[1]);
                fb.setCategory_id(split[2]);
                fb.setPrice(Float.parseFloat(split[3]));

                fb.setFlag("1");
            }

            k.set(pid);
            context.write(k, fb);
        }
    }

    static  class RJoinReducer extends Reducer<Text, InfoBean, InfoBean, NullWritable>
    {

        List<InfoBean> list = new  ArrayList<InfoBean>();
        InfoBean fb = new InfoBean();

        @Override
        protected void reduce(Text key, Iterable<InfoBean> values, Context context) throws IOException, InterruptedException {

            for(InfoBean o : values)
            {
                if("1".equals(o.getFlag()))
                {
                    try {
                        BeanUtils.copyProperties(fb, o);

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                else
                {
                    InfoBean ib = new InfoBean();
                    try {
                        BeanUtils.copyProperties(ib, o);

                        list.add(ib);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }

            for(InfoBean o : list)
            {
                o.setP_id(fb.getP_id());
                o.setPname(fb.getPname());
                o.setCategory_id(fb.getCategory_id());
                o.setPrice(fb.getPrice());

                context.write(o, NullWritable.get());
            }

        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();

        configuration.set("fs.defaultFs", "file:///");

        Job job = Job.getInstance(configuration);

        job.setJarByClass(RJoin.class);

        job.setMapperClass(RJoinMapper.class);
        job.setReducerClass(RJoinReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(InfoBean.class);

        job.setOutputKeyClass(InfoBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job,
                "E:\\IdeaProjects\\hadoopTest\\SrcInput\\JoinInput");
        FileOutputFormat.setOutputPath(job,
                new Path("E:\\IdeaProjects\\hadoopTest\\SrcInput\\output"));

        boolean b = job.waitForCompletion(true);

        System.exit(b?0:1);

    }

}
