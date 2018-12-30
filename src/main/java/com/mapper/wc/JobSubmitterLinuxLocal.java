package com.mapper.wc;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/*linux本地执行*/
public class JobSubmitterLinuxLocal {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();


        Job job = Job.getInstance(conf);

        //指定jar包的位置
        job.setJarByClass(JobSubmitterLinuxLocal.class);

        //指定mapper和reduce
        job.setMapperClass(WordcountMapper.class);
        job.setReducerClass(WordcountReducer.class);

        //指定mapper和reducer实现类产生结果数据的key,value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //指定路径
        FileInputFormat.setInputPaths(job,new Path("/"));
        FileOutputFormat.setOutputPath(job,new Path("/output"));

        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);

    }



}
