package com.lcy.hadoop.mr.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * Created by： luochengyue
 * date: 2019/5/7.
 * desc：
 *  Reducer<Text,IntWritable,Text,IntWritable>
 *  Reducer参数说明：key 对应map的输出key，value对应map的输出value，之后是对应本身reduce的输出参数key和value的类型
 * @version:
 */
public class WcReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //由于reduce相同key会被传输到同一个reducetask，之后一组相同的key会调用一次reduce所以 key对应的value是Iterable
        int count = 0;
        for(IntWritable vc:values){
            count+=vc.get();
        }
        context.write(key,new IntWritable(count));
    }
}
