package com.lcy.hadoop.mr.flowsum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by： luochengyue
 * date: 2019/5/10.
 * desc：map主要做拆分和获取对应数据做一个流量统计操作
 * @version:
 */
public class FlowSumMap extends Mapper<LongWritable,Text,Text,FlowBean> {
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
