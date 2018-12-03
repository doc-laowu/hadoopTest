package com.neusoft.sumFlow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;


public class ProvincePartitioner extends Partitioner<flowBean, Text> {

    public static HashMap<String, Integer> dict = new HashMap<>();

    static
    {
        dict.put("136", 0);
        dict.put("137", 1);
        dict.put("138", 2);
        dict.put("139", 3);
    }

    @Override
    public int getPartition(flowBean flowBean,Text text,  int i) {
        String substring = text.toString().substring(0, 3);
        Integer integer = dict.get(substring);

        return integer==null?4:integer;
    }
}
