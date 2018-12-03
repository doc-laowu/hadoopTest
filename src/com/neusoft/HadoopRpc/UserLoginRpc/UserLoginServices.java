package com.neusoft.HadoopRpc.UserLoginRpc;

public interface UserLoginServices
{
    public static final long versionID = 100L;

    public String Login(String user, String password);
}