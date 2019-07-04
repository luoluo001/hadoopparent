package com.lcy.hadoop.mr.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.File;
import java.io.IOException;

/**
 * Created by： luochengyue
 * date: 2019/5/7.
 * desc：yarn的客户端，
 * @version:
 */
public class WcDriver {

    public static void main(String[] args) throws Exception{
        //由于涉及到操作hdfs数据，有一个用户的概念所以需要设置下hadoop的用户名 -DHADOOP_USER_NAME=hadoop 就可以设置用户名了
        //这个可以查看FileSystem里面会发现获取一个系统内的HADOOP_USER_NAME作为用户名 job.setUser应该也可以回头试下
        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS","hdfs://hadoop1:9000");//可不配置默认操作本地的

        Job job = Job.getInstance(conf);
        //接着主要设置几个简单的参数即可 主要设置包括map类，reduce类，map输出参数类型，reduce最终输出类型，驱动所在的jar包
        //这些方法都可以在job上看到set相关的方法
        job.setJarByClass(WcDriver.class);//所在jar包
        job.setMapperClass(WcMapper.class);
        job.setReducerClass(WcReducer.class);
        //map输出的相关参数
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //reduce输出的相关参数
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //输入输出参数路径设置
        deleteFiles(new File(args[1]));
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //提交等待完成 会输出打印过程 实际调用的是submit
        boolean comple = job.waitForCompletion(true);
        System.exit(comple?0:1);
    }

    private static void deleteFiles(File file) {
        if(file.isDirectory()){
            File[] files = file.listFiles();
            for(File f:files){
                deleteFiles(f);
            }
        }
        file.delete();
    }
}
