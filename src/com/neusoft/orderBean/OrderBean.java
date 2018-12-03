package com.neusoft.orderBean;


import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

//public class OrderBean implements WritableComparable<OrderBean> {

public class OrderBean extends WritableComparator implements WritableComparable<OrderBean> {


    private int id;

    public int getId() {
        return id;
    }

    public double getPrice() {
        return price;
    }

    private double price;

    OrderBean() { }

    public OrderBean(int id, double price) {
        this.id = id;
        this.price = price;
    }

    public void set(int id, double price) {
        this.id = id;
        this.price = price;
    }

    @Override
    public int compareTo(OrderBean o) {
        if(this.id > o.id)
            return 1;
        else if(this.id < o.id)
            return -1;
        else
            return this.price - o.price > 0 ? -1 : 1;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
        out.writeDouble(price);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.price = in.readDouble();
    }

    @Override
    public String toString() {
        return id+"\t"+price;
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean x = (OrderBean)a;
        OrderBean y = (OrderBean)b;

        return x.getId() - y.getId();
    }
}
