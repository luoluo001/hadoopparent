package com.lcy.hadoop.mr.reducepartition;

import com.lcy.hadoop.mr.flowsum.FlowBean;
import com.lcy.hadoop.mr.flowsum.FlowSumDriver;
import com.lcy.hadoop.mr.flowsum.FlowSumMap;
import com.lcy.hadoop.mr.flowsum.FlowSumReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by： luochengyue
 * date: 2019/5/10.
 * desc：这个需求主要是根据手机号进行分区 这里做个简单版本的比如根据手机号前三位完成分区
 */
public class PhoneNumPartitionDriver {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://hadoop1:9000");//可不配置默认操作本地的
        Job job = Job.getInstance(conf);
        //接着主要设置几个简单的参数即可 主要设置包括map类，reduce类，map输出参数类型，reduce最终输出类型，驱动所在的jar包
        //这些方法都可以在job上看到set相关的方法
        job.setJarByClass(PhoneNumPartitionDriver.class);//所在jar包
        job.setMapperClass(PartitionMapper.class);
        job.setReducerClass(PartitionReducer.class);
        //map输出的相关参数
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        //reduce输出的相关参数
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        //设置partition类和对应的reduce数量
        job.setPartitionerClass(PhoneNumPartition.class);
        job.setNumReduceTasks(5);
        //输入输出参数路径设置
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //提交等待完成 会输出打印过程 实际调用的是submit
        boolean comple = job.waitForCompletion(true);
        System.exit(comple?0:1);
    }

    /**
     * 分区map，根据
     */
    static class PartitionMapper extends Mapper<LongWritable,Text,Text,FlowBean>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //先做下拆分
            String[] valuses = value.toString().split("\t");
            if(valuses.length>3){
                Long upFlow = Long.parseLong(valuses[valuses.length - 3]);
                Long downFlow = Long.parseLong(valuses[valuses.length - 2]);
                //输出到分区数据中
                context.write(new Text(valuses[1]),new FlowBean(upFlow,downFlow));
            }else{
                System.out.println("解析错误："+value.toString());
            }
        }
    }

    static class PartitionReducer extends Reducer<Text,FlowBean,Text,FlowBean>{
        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
            long upFlowSum = 0;
            long downFlowSum = 0;
            //做上行下载流量汇总
            for (FlowBean flowBean:values){
                upFlowSum+= flowBean.getUpFlum();
                downFlowSum+=flowBean.getDownFlum();
            }
            //按手机号与跟对应的总数据做输出
            context.write(key,new FlowBean(upFlowSum,downFlowSum));
        }
    }

    static class PhoneNumPartition extends HashPartitioner<Text,FlowBean>{
        static Map<String,Integer> partitionMap = new HashMap<>();
        static {
            partitionMap.put("134",0);
            partitionMap.put("135",1);
            partitionMap.put("136",2);
            partitionMap.put("137",3);
        }
        @Override
        public int getPartition(Text key, FlowBean value, int numReduceTasks) {
            Integer par = partitionMap.get(key.toString().substring(0, 3));
            if(par==null){
                return 4;
            }
            return par;
        }
    }
}
