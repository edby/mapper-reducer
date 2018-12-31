package com.mapper.flow;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean  implements Writable {
    private int upFlow;
    private int dFlow;
    private int amountFlow;
    private  String phone;

    public FlowBean() {}



    public FlowBean(String phone,int upFlow, int dFlow) {
        this.phone = phone;
        this.upFlow = upFlow;
        this.dFlow = dFlow;
        this.amountFlow=upFlow+dFlow;
    }

    public int getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(int upFlow) {
        this.upFlow = upFlow;
    }

    public int getdFlow() {
        return dFlow;
    }

    public void setdFlow(int dFlow) {
        this.dFlow = dFlow;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public int getAmountFlow() {
        return amountFlow;
    }

    public void setAmountFlow(int amountFlow) {
        this.amountFlow = amountFlow;
    }

    /*hadoop系统在序列化该类的对象时调用的方法*/
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(upFlow);
        out.writeUTF(phone);
        out.writeInt(dFlow);
        out.writeInt(amountFlow);
    }

    /*hadoop在反序列化该类的对象时调用的方法*/
    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow = in.readInt();
        this.phone = in.readUTF();
        this.dFlow = in.readInt();
        this.amountFlow = in.readInt();

    }


    @Override
    public String toString() {
        return this.phone + ","+this.upFlow +","+ this.dFlow +"," + this.amountFlow;
    }
}


