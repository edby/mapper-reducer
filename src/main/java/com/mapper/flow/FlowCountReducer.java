package com.mapper.flow;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean>{

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        //记数
        int upFlow = 0;
        int dFlow = 0;

        for (FlowBean value : values) {
                upFlow+=value.getUpFlow();
                dFlow+=value.getdFlow();
        }
        context.write(key, new FlowBean(key.toString(),upFlow,dFlow));
    }
}
