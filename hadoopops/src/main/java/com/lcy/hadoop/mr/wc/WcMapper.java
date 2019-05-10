package com.lcy.hadoop.mr.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by： luochengyue
 * date: 2019/5/7.
 * desc：
 *  Mapper<LongWritable,Text,Text,IntWritable>
 *  泛型的四个字段说明：读取数据的位置，读取的这一行数据，输出的Key的类型，输出的Value类型
 *  另外需要注意由于涉及到网络传输，所以需要使用hadoop包中的基本类型传输包，也可以自定义，只要实现对应的hadoop的Writable接口接口
 * @version:
 */
public class WcMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split(" ");
        //将每个单词和对应的计数输出，由于会根据key排序，所以我们中间也可以不进行数据统计
        for(String w:words){
            context.write(new Text(w),new IntWritable(1));
        }
    }
}
