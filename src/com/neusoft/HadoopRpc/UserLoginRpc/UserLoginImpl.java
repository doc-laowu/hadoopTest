package com.neusoft.HadoopRpc.UserLoginRpc;

public class UserLoginImpl implements UserLoginServices {


    @Override
    public String Login(String user, String password) {
        return user + ":" +password + "登录成功!";
    }
}
