package com.neusoft.orderBean;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class myGroupingComparator extends WritableComparator {

    public myGroupingComparator() {
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable  a, WritableComparable  b) {

        OrderBean x = (OrderBean)a;
        OrderBean y = (OrderBean)b;

        return x.getId() - y.getId();

    }
}
