package com.lcy.hadoop.mr.flowsum;

import com.lcy.hadoop.mr.wc.WcDriver;
import com.lcy.hadoop.mr.wc.WcMapper;
import com.lcy.hadoop.mr.wc.WcReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by： luochengyue
 * date: 2019/5/10.
 * desc：这个包主要是用于做流量统计用的，需求说明，通过日志文件统计上传/下载和总流量跟据每个手机号做汇总
 * @version:
 */
public class FlowSumDriver {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://hadoop1:9000");//可不配置默认操作本地的
        Job job = Job.getInstance(conf);
        //接着主要设置几个简单的参数即可 主要设置包括map类，reduce类，map输出参数类型，reduce最终输出类型，驱动所在的jar包
        //这些方法都可以在job上看到set相关的方法
        job.setJarByClass(FlowSumDriver.class);//所在jar包
        job.setMapperClass(FlowSumMap.class);
        job.setReducerClass(FlowSumReduce.class);
        //map输出的相关参数
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        //reduce输出的相关参数
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        //输入输出参数路径设置
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //提交等待完成 会输出打印过程 实际调用的是submit
        boolean comple = job.waitForCompletion(true);
        System.exit(comple?0:1);
    }
}
