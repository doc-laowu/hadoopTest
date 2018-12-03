package com.neusoft.HadoopRpc.UserLoginRpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

public class UserLoginAction {

    public static void main(String[] args) throws IOException {

        UserLoginServices localhost = RPC.getProxy(UserLoginServices.class, 1L,
                new InetSocketAddress("localhost", 8888), new Configuration());

        String laowu = localhost.Login("laowu", "123");

        System.out.println(laowu);

    }

}
