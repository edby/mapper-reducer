package com.mapper.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PageCountStep2 {

    public static class PageCountStep2Mapper extends Mapper<LongWritable,Text,pageCount,NullWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] split = line.split("\t");

            pageCount pageCount = new pageCount();
            pageCount.set(split[0],Integer.parseInt(split[1]));

            context.write(pageCount,NullWritable.get());
        }
    }


    public static class PageCountStep2Reducer extends Reducer<pageCount,NullWritable,pageCount,NullWritable> {
        @Override
        protected void reduce(pageCount key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key,NullWritable.get());
        }
    }


    public static void main(String[] args) throws Exception {

        /**
         * 通过加载classpath下的*-site.xml文件解析参数
         */
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(PageCountStep2.class);

        job.setMapperClass(PageCountStep2Mapper.class);
        job.setReducerClass(PageCountStep2Reducer.class);

        job.setMapOutputKeyClass(pageCount.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(pageCount.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job,new Path("E:/javaProject/testdata/output"));
        FileOutputFormat.setOutputPath(job,new Path("E:/javaProject/testdata/output1"));


        job.waitForCompletion(true);

    }

}
