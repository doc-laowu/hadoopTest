package com.neusoft.hadoopTest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

public class SequenceFileWriterDemo {

    private static final String[] DATA = {
            "one two, buckle my shoe",
            "three four, shut my door",
            "five six, pick up sticks",
            "seven eight, lay them stright",
            "nine ten, a big fat hen"
    };

    @Test
    public void sequenceWriter() throws IOException {
        String uri = "file:///E://IdeaProjects/hadoopTest/SrcInput/SequenceFile/B.txt";
        Configuration conf = new Configuration();
        Path path = new Path(uri);

        IntWritable key = new IntWritable();
        Text value = new Text();
        SequenceFile.Writer writer = null;
        SequenceFile.Writer.Option option1 = SequenceFile.Writer.file(path);
        SequenceFile.Writer.Option option2 = SequenceFile.Writer.keyClass(key.getClass());
        SequenceFile.Writer.Option option3 = SequenceFile.Writer.valueClass(value.getClass());

        try {

            writer = SequenceFile.createWriter(conf, option1, option2, option3,
                    SequenceFile.Writer.compression(SequenceFile.CompressionType.RECORD));

            for (int i = 0; i < 10; i++) {
                key.set(1 + i);
                value.set(DATA[i % DATA.length]);
                System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key,
                        value);
                writer.append(key, value);
            }
        } finally {
            IOUtils.closeStream(writer);
        }
    }

    @Test
    public void sequenceReader() throws IOException {
            String uri = "file:///E://IdeaProjects/hadoopTest/SrcInput/SequenceFile/B.txt";
            Configuration conf = new Configuration();
            Path path = new Path(uri);
            SequenceFile.Reader.Option option1 = SequenceFile.Reader.file(path);
            //SequenceFile.Reader.Option option2 = SequenceFile.Reader.length(174);//这个参数表示读取的长度

            SequenceFile.Reader reader = null;
            try {
                reader = new SequenceFile.Reader(conf,option1);
                Writable key = (Writable) ReflectionUtils.newInstance(
                        reader.getKeyClass(), conf);
                Writable value = (Writable) ReflectionUtils.newInstance(
                        reader.getValueClass(), conf);
                long position = reader.getPosition();
                while (reader.next(key, value)) {
                    String syncSeen = reader.syncSeen() ? "*" : "";
                    System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen, key,
                            value);
                    position = reader.getPosition(); // beginning of next record
                }
            } finally {
                IOUtils.closeStream(reader);
            }
        }
}
