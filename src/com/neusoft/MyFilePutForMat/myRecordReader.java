package com.neusoft.MyFilePutForMat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;

public class myRecordReader extends RecordReader<Text, Text> {

    private LineReader lr ;
    private Text key = new Text();
    private Text value = new Text();
    private long start ;
    private long end;
    private long currentPos;
    private Text line = new Text();

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit filesplit = (FileSplit) split;
        Configuration conf = context.getConfiguration();
        Path path = filesplit.getPath();
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream fsinput = fs.open(path);

        lr = new LineReader(fsinput, conf);

        start = filesplit.getStart();
        end = start + filesplit.getLength();
        fsinput.seek(start);

        if(start!=0){
            start += lr.readLine(new Text(),0,
                    (int)Math.min(Integer.MAX_VALUE, end-start));
        }
        currentPos = start;

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(currentPos > end){
            return false;
        }
        currentPos += lr.readLine(line);
        if(line.getLength()==0){
            return false;
        }
        if(line.toString().startsWith("ignore")){
            currentPos += lr.readLine(line);
        }

        String [] words = line.toString().split(",");
        // 异常处理
        if(words.length<2){
            System.err.println("line:"+line.toString()+".");
            return false;
        }
        key.set(words[0]);
        value.set(words[1]);
        return true;

    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (currentPos - start) / (float) (end - start));
        }

    }

    @Override
    public void close() throws IOException {
        lr.close();
    }
}
