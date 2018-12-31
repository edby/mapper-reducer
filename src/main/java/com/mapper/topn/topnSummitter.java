package com.mapper.topn;



import com.mapper.flow.JobSubmitter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class topnSummitter {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        /*设置一个参数,通过代码方式* */
        //conf.setInt("top.n",5);
        //设置一个参数,配置文件模式
        conf.addResource("xx-oo.xml");

        Job job = Job.getInstance(conf);
        job.setJarByClass(JobSubmitter.class);


        job.setJarByClass(JobSubmitter.class);

        job.setMapperClass(PageTopMapper.class);
        job.setReducerClass(PageTopReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //指定路径
        FileInputFormat.setInputPaths(job,new Path("E:/javaProject/testdata/input"));
        FileOutputFormat.setOutputPath(job,new Path("E:/javaProject/testdata/output"));

        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);


    }
}
