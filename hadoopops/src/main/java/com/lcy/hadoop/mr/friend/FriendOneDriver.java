package com.lcy.hadoop.mr.friend;

import java.io.IOException;

import com.lcy.hadoop.mr.utils.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by luo on 2019/6/2.
 */
public class FriendOneDriver {

    //A:b,c,d a有bcd几个好友,我们其实要找的是b的好友有谁c的好友有谁
    static class FriendOneMapper extends Mapper<LongWritable,Text,Text,Text> {
        Text fValue  = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println(value);
            String[] splictValuse = value.toString().split(":");
            String keyStr = splictValuse[0];
            //获取所有以此为好友的
            String[] frieds = splictValuse[1].split(",");
            fValue.set(keyStr);
            for(String s:frieds){
                context.write(new Text(s),fValue);
            }
        }
    }
    //
    static class FriendOneReducer extends Reducer<Text,Text,Text,Text> {
        /**
         * 现在获取到的就是以key为共同好友的一组人员数据，我们先求出key的共同好友都有谁
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for(Text v:values){
                sb.append(v.toString()).append(",");
            }
            context.write(key,new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","file:///");
        conf.set("mapreduce.framework.name","local");
        //操作本地可不设置任何参数,如果以yarn的形式去提交任务则需要设置执行框架为yarn并且需要配置文件系统为hdfs
        Job job = Job.getInstance(conf);
        job.setJarByClass(FriendOneDriver.class);

        job.setMapperClass(FriendOneMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(FriendOneReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileUtils.deleteFile(args[1]);
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean isSuccess = job.waitForCompletion(true);
        System.exit(isSuccess?0:1);
    }


}
