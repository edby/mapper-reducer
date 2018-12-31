package com.mapper.topn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class PageTopReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

    TreeMap<pageCount,Object> treeMap=new TreeMap<>();


    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count=0;
        for (IntWritable value : values) {
            count+=value.get();
        }
        pageCount pageC = new pageCount();
        pageC.set(key.toString(),count);
        treeMap.put(pageC,null);
    }

    /*所有数据都处理完了调用这个方法*/
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        //获取配置参数，取前面几个
        Configuration conf = context.getConfiguration();
        int topn = conf.getInt("top.n",5);

        Set<Map.Entry<pageCount, Object>> entries = treeMap.entrySet();
        int i=0;

        for (Map.Entry<pageCount, Object> entry : entries) {
                context.write(new Text(entry.getKey().getPage()),new IntWritable(entry.getKey().getCount()));
                i++;
                if(i==topn) return;
        }

    }


}
