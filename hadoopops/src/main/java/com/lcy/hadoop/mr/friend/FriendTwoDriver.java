package com.lcy.hadoop.mr.friend;

import java.io.IOException;
import java.util.Arrays;

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
public class FriendTwoDriver {

    //将key为公共好友的 两两组成一队,value以当前的共同好友为value
    static class FriendTwoMapper extends Mapper<LongWritable,Text,Text,Text> {
        Text fValue  = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splicts = value.toString().split("\t");
            fValue.set(splicts[0]);
            String[] commomFs = splicts[1].split(",");
            Arrays.sort(commomFs);
            String twoPerson = null;
            for(int i = 0;i<commomFs.length-1;i++){
                for(int j = i +1;j<commomFs.length;j++){
                    twoPerson = commomFs[i] + "--" + commomFs[j];
                    context.write(new Text(twoPerson),fValue);
                }
            }
        }
    }
    //
    static class FriendTwoReducer extends Reducer<Text,Text,Text,Text> {
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
            String result = sb.toString();
            if(sb.length()>1){
                result = sb.substring(0,sb.length()-1);
            }
            context.write(key,new Text(result));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","file:///");
        conf.set("mapreduce.framework.name","local");
        //操作本地可不设置任何参数,如果以yarn的形式去提交任务则需要设置执行框架为yarn并且需要配置文件系统为hdfs
        Job job = Job.getInstance(conf);
        job.setJarByClass(FriendTwoDriver.class);

        job.setMapperClass(FriendTwoMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(FriendTwoReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setCombinerClass(FriendTwoReducer.class);//由于不影响最终结果所以这里以combiner可以设置，可以提升reduce效率
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileUtils.deleteFile(args[1]);
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean isSuccess = job.waitForCompletion(true);
        System.exit(isSuccess?0:1);
    }
}
