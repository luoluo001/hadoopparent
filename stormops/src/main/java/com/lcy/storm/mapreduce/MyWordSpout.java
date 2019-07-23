package com.lcy.storm.mapreduce;

import org.apache.storm.lambda.SerializableSupplier;
import org.apache.storm.spout.ISpoutOutputCollector;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by： luo
 * date: 2019/7/23.
 * desc：spout组件 用来获取外部数据源的
 */
public class MyWordSpout extends BaseRichSpout {
    //挺眼熟 这个是输出组件
    SpoutOutputCollector collector;
    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        collector.emit(new Values("you will get me every time."));
        //这个是获取tuple的方法 每一次都发射这个数据，当然你也可以用其他的方法发送不同的数据
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //用于做字段声明的，对应values 上面的字段，这个也是list
        outputFieldsDeclarer.declare(new Fields("allword"));
    }
}
