package com.neusoft.DouBanMovies;

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

public class MovieMaxVotes {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFs", "file:///");

        Job job = Job.getInstance(conf);

        job.setJarByClass(MovieMaxVotes.class);
        job.setMapperClass(MovieMaxVotesMap.class);
        job.setReducerClass(MovieMaxVotesReduce.class);

        job.setMapOutputKeyClass(MovieBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(MovieBean.class);
        job.setOutputValueClass(NullWritable.class);

        job.setGroupingComparatorClass(GroupComparator.class);

        job.setNumReduceTasks(1);

        Path path = new Path("E:\\IdeaProjects\\hadoopTest\\output\\MinVotesTop10");
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

    static class MovieBean implements WritableComparable<MovieBean> {

        private String MovieName;
        private long votes;

        public MovieBean() {
        }

        public String getMovieName() {
            return MovieName;
        }

        public void setMovieName(String movieName) {
            MovieName = movieName;
        }

        public long getVotes() {
            return votes;
        }

        public void setVotes(long votes) {
            this.votes = votes;
        }

        @Override
        public int compareTo(MovieBean o) {
            return this.votes - o.getVotes() > 0 ? 1 : -1;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(this.MovieName);
            out.writeLong(this.votes);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.MovieName = in.readUTF();
            this.votes = in.readLong();
        }

        @Override
        public String toString() {
            return MovieName+"\t"+votes;
        }
    }

    static class MovieMaxVotesMap extends Mapper<LongWritable, Text, MovieBean, NullWritable> {

        MovieBean MB = new MovieBean();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] s = value.toString().split("\t");
            MB.setMovieName(s[0]);
            MB.setVotes(Long.parseLong(s[8]));

            context.write(MB, NullWritable.get());
        }
    }

    static  class MovieMaxVotesReduce extends Reducer<MovieBean, NullWritable, MovieBean, NullWritable> {

        @Override
        protected void reduce(MovieBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

            int i = 1;
            for(NullWritable o : values){
                context.write(key, o);
                if(i++ == 10){
                    break;
                }
            }
        }
    }

    static class GroupComparator extends WritableComparator {

        public GroupComparator() {
            super(MovieBean.class, true);
        }

        @Override
        public int compare(WritableComparable  a, WritableComparable  b) {
            return 0;
        }
    }

}
