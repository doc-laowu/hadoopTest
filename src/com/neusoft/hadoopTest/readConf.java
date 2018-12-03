package com.neusoft.hadoopTest;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class readConf {

    @Test
    public void read() {

        Configuration conf = new Configuration();
        conf.addResource("configuration-1.xml");

        System.out.println(conf.get("color"));
        System.out.println(conf.get("size"));
        System.out.println(conf.get("breadth", "wide"));

    }

}
