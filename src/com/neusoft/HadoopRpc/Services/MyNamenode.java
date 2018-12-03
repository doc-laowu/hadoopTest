package com.neusoft.HadoopRpc.Services;

public class MyNamenode implements ClientNamenodeProtocal {

    //模拟namenode的业务方法
    @Override
    public String getMataData(String path)
    {
        return path+"+3 - {BLK_3, BLK_2} ... ";
    }

}
