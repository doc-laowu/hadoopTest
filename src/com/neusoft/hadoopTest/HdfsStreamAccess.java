package com.neusoft.hadoopTest;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsStreamAccess {


    Configuration configuration = null;
    FileSystem fileSystem = null;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        configuration = new Configuration();
        fileSystem = FilterFileSystem.get(new URI
                ("hdfs://192.168.64.131:9000"), configuration, "hadoop");
    }

    /**
     * 通过流的方式创建文件夹
     * @throws IOException
     */
    @Test
    public void TestUpload() throws IOException {
        FSDataOutputStream fsDataOutputStream =
                fileSystem.create(new Path("/streamFile"), true);

        FileInputStream fileInputStream = new FileInputStream("E:\\file.txt");

        IOUtils.copy(fileInputStream, fsDataOutputStream);

    }

    /**
     * 通过流的方式下载文件夹
     */

    @Test
    public void TestDownLoad() throws IOException {

        FSDataInputStream open = fileSystem.open(new Path("/echo.txt"));

        FileOutputStream fileOutputStream = new FileOutputStream("E:\\myfile");

        IOUtils.copy(open, fileOutputStream);
    }

    @Test
    public void TestRandomAccess() throws IOException {
        FSDataInputStream open = fileSystem.open(new Path("/echo.txt"));
        FileOutputStream fileOutputStream = new FileOutputStream("E:\\myFile");

        open.seek(12);

        IOUtils.copy(open, fileOutputStream);
    }

    /**
     * 显示文件内容到屏幕上
     * @throws IOException
     */
    @Test
    public void catFile() throws IOException {
        FSDataInputStream open = fileSystem.open(new Path("/echo.txt"));
        IOUtils.copy(open, System.out);
    }


}
