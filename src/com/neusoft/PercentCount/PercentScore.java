package com.neusoft.PercentCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PercentScore {

    static class infobean implements Writable{

        private String year;
        private String contry;
        private String level;

        public String getLevel() {
            return level;
        }

        public void setLevel(String level) {
            this.level = level;
        }

        public infobean() {
        }

        public String getYear() {
            return year;
        }

        public void setYear(String year) {
            this.year = year;
        }

        public String getContry() {
            return contry;
        }

        public void setContry(String contry) {
            this.contry = contry;
        }


        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(this.year);
            out.writeUTF(this.contry);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.year = in.readUTF();
            this.contry = in.readUTF();
        }

        @Override
        public String toString() {
            return year +"\t"+ contry+"\t"+level;
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFs", "file:///");

        Job job = Job.getInstance(conf);

        job.setJarByClass(PercentScore.class);
        job.setMapperClass(PercentScoreMap.class);
        job.setReducerClass(PercentScoreReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setCombinerClass(PercentScoreReduce.class);

        Path path = new Path("E:\\IdeaProjects\\hadoopTest\\output1\\MoviesPercentScore");
        FileSystem fs = FileSystem.get(conf);

        if(fs.exists(path))
        {
            fs.delete(path, true);
        }

        FileInputFormat.setInputPaths(job, "E:\\IdeaProjects\\hadoopTest\\SrcInput\\DoubanInput");
        FileOutputFormat.setOutputPath(job, path);

        boolean b = job.waitForCompletion(true);

        System.exit(b?0:1);

    }


    static class PercentScoreMap extends Mapper<LongWritable, Text, Text, IntWritable>{

        Text k = new Text();
        IntWritable v = new IntWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] split = value.toString().split("\t");
            float score = Float.parseFloat(split[7]);
            if(score >0 && score <= 5){
                k.set("0~5");
            }else if(score >5 && score <= 6){
                k.set("5~6");
            }else if(score > 6 && score <= 7){
                k.set("6~7");
            }else if(score > 7 && score <= 8){
                k.set("7~8");
            }else if(score > 8 && score <= 9){
                k.set("8~9");
            }else if(score > 9 && score <= 10){
                k.set("9~10");
            }

            context.write(k, v);
        }
    }

    static class PercentScoreReduce extends Reducer<Text, IntWritable, Text, IntWritable>{

        IntWritable v = new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int total = 0;
            for(IntWritable o : values){
                total += o.get();
            }
            v.set(total);
            context.write(key, v);
        }
    }

}
