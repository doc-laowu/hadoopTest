package com.neusoft.hadoopTest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyDefault;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.Map;

public class Main {


    Configuration configuration = null;
    FileSystem fileSystem = null;

    @Before
    public void getFileSystem() throws URISyntaxException, IOException, InterruptedException {
        configuration = new Configuration();
        fileSystem = FilterFileSystem.get(new URI
                ("hdfs://192.168.64.131:9000"), configuration, "hadoop");
        System.out.println(fileSystem.toString());
        fileSystem.close();
    }

    @Test
    public void upFile() throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();
        configuration.addResource(new Path("/usr/local/hadoop-2.7.6/etc/hadoop/hdfs-site.xml"));
        FileSystem fileSystem = FilterFileSystem.get(new URI
                ("hdfs://192.168.64.131:9000"), configuration, "hadoop");
        fileSystem.copyFromLocalFile(new Path("E:\\LearnFile\\laowu_jianli.pdf")
                , new Path("/jianli.pdf"));
        fileSystem.close();
    }

    @Test
    public void upFileByIo() throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FilterFileSystem.get(new URI
                ("hdfs://192.168.64.131:9000"), configuration, "hadoop");

        try (
                FileInputStream fin = new FileInputStream("E:\\LearnFile\\jianli_2.pdf")
        )
        {
                FSDataOutputStream fsDataOutputStream =
                        fileSystem.create(new Path("/jianli_2.pdf"), new Progressable() {

                            //重写Progressable接口可以在上传文件的时候给出提示

                            @Override
                            public void progress() {
                                System.out.print(".");
                            }
                        });

                IOUtils.copyBytes(fin, fsDataOutputStream, configuration);
                IOUtils.closeStream(fin);
                IOUtils.closeStream(fsDataOutputStream);
        }
    }


    @Test
    public void DownFileByIo() throws URISyntaxException, IOException, InterruptedException
    {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FilterFileSystem.get(new URI
                ("hdfs://192.168.64.131:9000"), configuration, "hadoop");

            FileOutputStream fou = new FileOutputStream("E:\\LearnFile\\jianli_2.pdf");
            FSDataInputStream open = fileSystem.open(new Path("/jianli_2.pdf"));
            IOUtils.copyBytes(open, fou, configuration);
            IOUtils.closeStream(fou);
            IOUtils.closeStream(open);
    }

    @Test
    public void downFile() throws InterruptedException, IOException, URISyntaxException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FilterFileSystem.get(new URI
                ("hdfs://192.168.64.131:9000"), configuration, "hadoop");
        fileSystem.copyToLocalFile(false
                ,new Path("/log4j.properties")
                ,new Path("E:/LearnFile/log4j.properties")
                , true);
        fileSystem.close();
    }




    @Test
    public void listFiles() throws InterruptedException, IOException, URISyntaxException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FilterFileSystem.get(new URI
                ("hdfs://192.168.64.131:9000"), configuration, "hadoop");

        FileStatus[] filestatus = fileSystem.listStatus(new Path("/"));

        for (FileStatus file : filestatus) {
            if (file.isDirectory())
                System.out.println(file.getPath().getName() + "是目录");
            else
                System.out.println(file.getPath().getName() + "是文件");
        }

        fileSystem.close();
    }

    @Test
    public void listFileProperty() throws InterruptedException, IOException, URISyntaxException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FilterFileSystem.get(new URI
                ("hdfs://192.168.64.131:9000"), configuration, "hadoop");
        RemoteIterator<LocatedFileStatus> localFilesItr =
                fileSystem.listFiles(new Path("/"), true);

        while(localFilesItr.hasNext())
        {
            LocatedFileStatus file = localFilesItr.next();
            System.out.println(file.getPath().getName()+"文件属性");
            System.out.println(file.getAccessTime());
            System.out.println(file.getPermission());
            System.out.println(file.getReplication());

            System.out.println("文件的块信息");
            BlockLocation[] blockLocations = file.getBlockLocations();
            for(BlockLocation b : blockLocations)
            {
                String[] names = b.getNames();
                for(String o : names)
                    System.out.println(o);

                String[] hosts = b.getHosts();
                for(String o : hosts)
                    System.out.println(o);

                String[] cachedHosts = b.getCachedHosts();
                for(String o :  cachedHosts)
                    System.out.println(o);

                System.out.println("文件大小为:"+b.getLength());
                System.out.println("块的开始的大小位置:"+b.getOffset());

                String[] topologyPaths = b.getTopologyPaths();
                for(String o : topologyPaths)
                    System.out.println(o);
            }
        }
    }

    @Test
    public void getConf()
    {
        Iterator<Map.Entry<String, String>> iterator = configuration.iterator();

        while(iterator.hasNext())
        {
            Map.Entry<String, String> next = iterator.next();
            System.out.println(next.getKey()+" : "+next.getValue());
        }
    }

    @Test
    public void mkdir() throws IOException {
        configuration.set("fs.defaultFS", "file:///");
        FileSystem fileSystem = FilterFileSystem.get(configuration);
        fileSystem.mkdirs(new Path("/aaa/bbb/ccc"));
    }

    @Test
    public void deleteDir() throws IOException {
        configuration.set("fs.defaultFS", "file:///");
        FileSystem fileSystem = FilterFileSystem.get(configuration);
        fileSystem.delete(new Path("/aaa/bbb/ccc"), true);
    }

}
