package com.neusoft.MyFilePutForMat;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FilterOutPutFormat<K, V> extends FileOutputFormat<K, V> {

    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {

        return new FilterRecord<K, V>(job);
    }
}
