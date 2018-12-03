package com.neusoft.MyFilePutForMat;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * 自定义一个InputFormat
 * 改写RecordReader，实现一次读取一个完整文件封装为KV
 * 在输出时使用SequenceFileOutPutFormat输出合并文件
 * @param <K>
 * @param <V>
 */


public class myFileInputFormat<K, V> extends FileInputFormat<K, V> {

    @Override
    public RecordReader<K, V> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        //return new myRecordReader();
        return null;
    }

}
