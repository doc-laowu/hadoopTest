package com.neusoft.DouBanMovies;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SortByValue {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFs", "file:///");

        Job job = Job.getInstance(conf);

        job.setJarByClass(SortByValue.class);
        job.setMapperClass(SortByValueMap.class);
        job.setReducerClass(SortByValueReduce.class);

        job.setMapOutputKeyClass(sortbean.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(sortbean.class);
        job.setOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(1);

        Path path = new Path("E:\\IdeaProjects\\hadoopTest\\output\\VotesGroupByYear");
        FileSystem fs = FileSystem.get(conf);

        if(fs.exists(path))
        {
            fs.delete(path, true);
        }

        FileInputFormat.setInputPaths(job, "E:\\IdeaProjects\\hadoopTest\\output\\sort\\");
        FileOutputFormat.setOutputPath(job, path);

        boolean b = job.waitForCompletion(true);

        System.exit(b?0:1);

    }

    static class sortbean implements WritableComparable<sortbean>{

        private String k;
        private long v;

        public sortbean() {
        }

        public String getK() {
            return k;
        }

        public void setK(String k) {
            this.k = k;
        }

        public long getV() {
            return v;
        }

        public void setV(long v) {
            this.v = v;
        }

        @Override
        public int compareTo(sortbean o) {
            return this.v - o.getV() > 0 ? 1 : -1;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(this.k);
            out.writeLong(this.v);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.k = in.readUTF();
            this.v = in.readLong();
        }

        @Override
        public String toString() {
            return k+"\t"+v;
        }
    }

    static class SortByValueMap extends Mapper<LongWritable, Text, sortbean, NullWritable> {

        sortbean k = new sortbean();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] split = value.toString().split("\t");
            k.setK(split[0]);
            k.setV(Long.parseLong(split[1]));
            context.write(k, NullWritable.get());
        }
    }

    static class SortByValueReduce extends Reducer<sortbean, NullWritable, sortbean, NullWritable>{

        @Override
        protected void reduce(sortbean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

            context.write(key, NullWritable.get());
        }
    }

}
