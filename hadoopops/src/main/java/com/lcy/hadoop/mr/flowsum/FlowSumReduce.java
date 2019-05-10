package com.lcy.hadoop.mr.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by： luochengyue
 * date: 2019/5/10.
 * desc：
 *
 * @version:
 */
public class FlowSumReduce extends Reducer<Text,FlowBean,Text,FlowBean>{
    /**
     * 根据对应的key 手机号，做下流量汇总，之后输出到reduce中
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
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
