package com.lcy.storm.mapreduce;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * Created by： luo
 * date: 2019/7/23.
 * desc：topology的入口
 */
public class MapReduceTopologyMain {

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("words", new MyWordSpout(), 2);
        builder.setBolt("splict", new MysplictBolt(), 4)
                .shuffleGrouping("words");
        builder.setBolt("wordc", new WordCountBolt(), 4)
                .fieldsGrouping("splict",new Fields("word"));//根据哪个字段去执行这个分组策略
        StormTopology topology = builder.createTopology();
        Config config =  new Config();
        config.setNumWorkers(2);
        //本地模式，还有一个本地的drpc模式
//        LocalCluster localCluster = new LocalCluster();
//        localCluster.submitTopology("mywordcount",config,topology);
        //storm的集群模式
        StormSubmitter.submitTopology("mywordcount",config,topology);
    }

}
