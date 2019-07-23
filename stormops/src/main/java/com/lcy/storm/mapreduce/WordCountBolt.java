package com.lcy.storm.mapreduce;

import org.apache.storm.lambda.SerializableBiConsumer;
import org.apache.storm.lambda.SerializableConsumer;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IWindowedBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by： luo
 * date: 2019/7/23.
 * desc：
 */
public class WordCountBolt extends BaseRichBolt {
    Map<String,Integer> wcCountMap = new ConcurrentHashMap<>();
    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {

    }

    @Override
    public void execute(Tuple input) {
        String word = input.getString(0);
        Integer num = input.getInteger(1);
        if(wcCountMap.get(word)!=null){
            wcCountMap.put(word,wcCountMap.get(word)+num);
        }else {
            wcCountMap.put(word,num);
        }
        System.out.println(word+":count=" +wcCountMap.get(word));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //不输出也不需要去声明了
    }
}
