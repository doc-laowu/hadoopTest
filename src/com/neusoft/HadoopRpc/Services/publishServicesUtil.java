package com.neusoft.HadoopRpc.Services;

import com.neusoft.HadoopRpc.UserLoginRpc.UserLoginImpl;
import com.neusoft.HadoopRpc.UserLoginRpc.UserLoginServices;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

public class publishServicesUtil {

    public static void main(String[] args) throws IOException {

//        RPC.Builder builder = new RPC.Builder(new Configuration());
//        builder.setBindAddress("localhost")
//                .setPort(8888)
//                .setProtocol(ClientNamenodeProtocal.class)
//                .setInstance(new MyNamenode());
//
//        RPC.Server build = builder.build();
//        build.start();

        RPC.Builder builder = new RPC.Builder(new Configuration());
        builder.setBindAddress("localhost")
                .setPort(8888)
                .setProtocol(UserLoginServices.class)
                .setInstance(new UserLoginImpl());

        RPC.Server build = builder.build();
        build.start();

    }

}
