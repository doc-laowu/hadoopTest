package com.neusoft.MyFilePutForMat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class FilterRecord<K, V> extends RecordWriter<K, V> {

    FSDataOutputStream fsBaidu = null;
    FSDataOutputStream fsOther = null;

    FilterRecord(TaskAttemptContext job) throws IOException {

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        fsBaidu = fs.create(new Path("E:\\IdeaProjects\\hadoopTest\\SrcInput\\Urls_Output\\baidu.log"));
        fsOther = fs.create(new Path("E:\\IdeaProjects\\hadoopTest\\SrcInput\\UrlS_Output\\other.log"));
    }


    @Override
    public void write(K key, V value) throws IOException, InterruptedException {
        if(key.toString().contains("baidu"))
        {
            fsBaidu.write(key.toString().getBytes());
            fsBaidu.write("\r\n".getBytes("utf-8"));
        }
        else
        {
            fsOther.write(key.toString().getBytes());
            fsOther.write("\r\n".getBytes("utf-8"));
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        if(fsBaidu != null)
        {
            fsBaidu.close();
        }
        if(fsOther != null)
        {
            fsOther.close();
        }
    }
}
