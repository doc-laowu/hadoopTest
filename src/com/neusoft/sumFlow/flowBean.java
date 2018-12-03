package com.neusoft.sumFlow;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class flowBean implements WritableComparable<flowBean> {

    private long upFlow = 0;
    private long downFlow = 0;
    private long sumFlow = 0;

    public flowBean() {}

    public flowBean(long upFlow, long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void set(long upFlow, long downFlow) {
        this.downFlow = downFlow;
        this.upFlow = upFlow;
        this.sumFlow = upFlow + downFlow;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.upFlow = dataInput.readLong();
        this.downFlow = dataInput.readLong();
        this.sumFlow = dataInput.readLong();
    }

    @Override
    public String toString() {
        return upFlow +", " +downFlow +", "+ sumFlow;
    }

    @Override
    public int compareTo(flowBean o) {
        return this.sumFlow > o.sumFlow?-1:1;
    }
}
