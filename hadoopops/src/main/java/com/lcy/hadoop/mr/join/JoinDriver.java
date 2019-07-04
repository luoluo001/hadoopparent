package com.lcy.hadoop.mr.join;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.lcy.hadoop.mr.flowsum.FlowBean;
import org.apache.commons.beanutils.BeanUtils;
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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by luo on 2019/6/2.
 */
public class JoinDriver {

    static class JoinMapper extends Mapper<LongWritable,Text,IntWritable,JoinBean>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splictValuse = value.toString().split(",");
            //通过文件名称来判断该文件时class还是student,这个信息在构建mr之前会放入到context中
            FileSplit splict = (FileSplit)context.getInputSplit();
            String name = splict.getPath().getName();
            JoinBean joinBean = null;
            int cid;
            if(name.contains("class")){
                //如果是class则设置class相关信
                joinBean = new JoinBean(-1,"",-1,Integer.valueOf(splictValuse[0]),splictValuse[1],true);
                cid = Integer.valueOf(splictValuse[0]);
            }else{
                //这部分是student的信息
                joinBean = new JoinBean(Integer.valueOf(splictValuse[0]),splictValuse[1],Integer.valueOf(splictValuse[2]),Integer.valueOf(splictValuse[3]),"",false);
                cid = Integer.valueOf(splictValuse[3]);
            }
            context.write(new IntWritable(cid),joinBean);
        }
    }
    //reducer主要做数据join之后的输出操作
    static class JoinReducer extends Reducer<IntWritable,JoinBean,JoinBean,NullWritable>{
        @Override
        protected void reduce(IntWritable key, Iterable<JoinBean> joinBeans, Context context) throws IOException, InterruptedException {
            //先找出对应的class和student做区分
            JoinBean classBean = new JoinBean();
            List<JoinBean> studentBenas = new ArrayList<>();
            try {
                for(JoinBean bean:joinBeans){
                    //这里是挨个序列化，所以bean实际上以最后一个bean的数据会覆盖前面的需要做下拷贝
                    if(bean.isClassFlas()){
                        BeanUtils.copyProperties(classBean,bean);
                    }else{
                        JoinBean sBean = new JoinBean();
                        BeanUtils.copyProperties(sBean,bean);
                        studentBenas.add(sBean);
                    }
                }
            }catch (Exception e){
                e.printStackTrace();
            }
            for(JoinBean bean:studentBenas){
                bean.setCName(classBean.getCName());
                context.write(bean,NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","file:///");
        conf.set("mapreduce.framework.name","local");
        //操作本地可不设置任何参数,如果以yarn的形式去提交任务则需要设置执行框架为yarn并且需要配置文件系统为hdfs
        Job job = Job.getInstance(conf);
        job.setJarByClass(JoinDriver.class);

        job.setMapperClass(JoinMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(JoinBean.class);

        job.setReducerClass(JoinReducer.class);
        job.setOutputKeyClass(FlowBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        deleteFIle(args[1]);
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean isSuccess = job.waitForCompletion(true);
        System.exit(isSuccess?0:1);
    }

    private static void deleteFIle(String arg) {
        File file = new File(arg);
        if(file.exists()){
            if(file.isDirectory()){
                String[] files = file.list();
                for(String f : files){
                    File fi = new File(file.getParent(),f);
                    fi.delete();
                }
                file.delete();
            }else{
                file.delete();
            }
        }
    }
}
