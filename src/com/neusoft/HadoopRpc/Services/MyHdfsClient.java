package com.neusoft.HadoopRpc.Services;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

public class MyHdfsClient {

    public static void main(String[] args) throws IOException {

        ClientNamenodeProtocal namenode = RPC.getProxy(ClientNamenodeProtocal.class, 1L,
                new InetSocketAddress("localhost", 8888), new Configuration());

        String mataData = namenode.getMataData("/angela.mygirl");
        System.out.println(mataData);
    }

}
