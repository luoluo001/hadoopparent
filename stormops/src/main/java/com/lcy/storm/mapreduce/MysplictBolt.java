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
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by： luo
 * date: 2019/7/23.
 * desc：第一个bolt，也是需要继承bolt基础类滴，否则需要一堆接口需要实现
 */
public class MysplictBolt extends BaseRichBolt {
    private OutputCollector collector;
    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        //这里由于上方下来的就是字符串，所以我们也是获取字符串就行 也可以getValues获取object 然后强转，如果传的是对象的话
        String allWord = input.getString(0);
        String[] words = allWord.split(" ");
        for(String w :words){
            collector.emit(new Values(w,1));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //主要做个示范这个可以传多个数并且做多个声明
        declarer.declare(new Fields("word","num"));
    }
}
