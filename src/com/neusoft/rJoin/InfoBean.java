package com.neusoft.rJoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class InfoBean implements Writable {

    private String order_id;
    private String dateString;
    private String p_id;
    private int amount;
    private String pname;
    private String category_id;
    private float price;

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    private String flag;

    public InfoBean() {
    }

    public InfoBean(String order_id, String dateString, String p_id, int amount, String pname, String category_id, float price) {
        this.order_id = order_id;
        this.dateString = dateString;
        this.p_id = p_id;
        this.amount = amount;
        this.pname = pname;
        this.category_id = category_id;
        this.price = price;
    }

    public String getOrder_id() {
        return order_id;
    }

    public void setOrder_id(String order_id) {
        this.order_id = order_id;
    }

    public String getDateString() {
        return dateString;
    }

    public void setDateString(String dateString) {
        this.dateString = dateString;
    }

    public String getP_id() {
        return p_id;
    }

    public void setP_id(String p_id) {
        this.p_id = p_id;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    public String getCategory_id() {
        return category_id;
    }

    public void setCategory_id(String category_id) {
        this.category_id = category_id;
    }

    public float getPrice() {
        return price;
    }

    public void setPrice(float price) {
        this.price = price;
    }

    /**
     *     private int order_id;
     *     private String dateString;
     *     private int p_id;
     *     private int amount;
     *     private String pname;
     *     private int category_id;
     *     private float price;
     * @param out
     * @throws IOException
     */


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(order_id);
        out.writeUTF(dateString);
        out.writeUTF(p_id);
        out.writeInt(amount);
        out.writeUTF(pname);
        out.writeUTF(category_id);
        out.writeFloat(price);
        out.writeUTF(flag);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.order_id = in.readUTF();
        this.dateString = in.readUTF();
        this.p_id = in.readUTF();
        this.amount = in.readInt();
        this.pname = in.readUTF();
        this.category_id = in.readUTF();
        this.price = in.readFloat();
        this.flag = in.readUTF();
    }

    @Override
    public String toString() {
        return this.order_id
                +"\t"+this.dateString+"\t"
                +this.pname+"\t"
                +this.category_id+"\t"
                +this.price+"\t"
                +this.amount;
    }
}
