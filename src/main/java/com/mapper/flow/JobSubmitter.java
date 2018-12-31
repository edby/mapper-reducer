package com.mapper.flow;

import com.mapper.wc.WordcountMapper;
import com.mapper.wc.WordcountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*上行流量和下行流量总和*/
public class JobSubmitter {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(JobSubmitter.class);

        //指定mapper和reduce
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);

        // 设置参数：maptask在做数据分区时，用哪个分区逻辑类  （如果不指定，它会用默认的HashPartitioner）
        job.setPartitionerClass(ProvincePartitioner.class);
        // 由于我们的ProvincePartitioner可能会产生6种分区号，所以，需要有6个reduce task来接收
        job.setNumReduceTasks(6);


        //指定mapper和reducer实现类产生结果数据的key,value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);


        //指定路径
        FileInputFormat.setInputPaths(job,new Path("E:/javaProject/testdata/input"));
        FileOutputFormat.setOutputPath(job,new Path("E:/javaProject/testdata/output"));

        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);


    }

}
