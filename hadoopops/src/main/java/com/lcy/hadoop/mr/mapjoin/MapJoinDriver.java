package com.lcy.hadoop.mr.mapjoin;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import com.lcy.hadoop.mr.join.JoinBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by luo on 2019/6/2.
 */
public class MapJoinDriver {

    static class MapJoinMapper extends Mapper<LongWritable,Text,JoinBean,NullWritable> {
        Map<Integer,String> classMap = new HashMap();
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            BufferedReader reader = null;
            try {
                reader = new BufferedReader(new InputStreamReader(new FileInputStream("join_class.txt")));
                String line = null;
                String[] valuse;
                while ((line = reader.readLine())!=null){
                    valuse = line.split(",");
                    classMap.put(Integer.valueOf(valuse[0]),valuse[1]);
                }

            }finally {
                reader.close();
            }

        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splictValuse = value.toString().split(",");
            //通过文件名称来判断该文件时class还是student,这个信息在构建mr之前会放入到context中
            FileSplit splict = (FileSplit)context.getInputSplit();
            String name = splict.getPath().getName();
            JoinBean joinBean = null;
            int cid;
            if(name.contains("student")){
                joinBean = new JoinBean(Integer.valueOf(splictValuse[0]),splictValuse[1],Integer.valueOf(splictValuse[2]),Integer.valueOf(splictValuse[3]),"",false);
                cid = Integer.valueOf(splictValuse[3]);
                joinBean.setCName(classMap.get(joinBean.getCId()));
                context.write(joinBean,NullWritable.get());
            }

        }
    }



    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","file:///");
        conf.set("mapreduce.framework.name","local");
        //操作本地可不设置任何参数,如果以yarn的形式去提交任务则需要设置执行框架为yarn并且需要配置文件系统为hdfs
        Job job = Job.getInstance(conf);
        job.setJarByClass(MapJoinDriver.class);
        job.addCacheFile(new URI("file:/E:/mr/join/input/join_class.txt"));
        job.setMapperClass(MapJoinMapper.class);
        job.setMapOutputKeyClass(JoinBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(JoinBean.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(0);
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
