package com.z.storm.wordcount;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

/* 
 ** WordCountTopolopgyAllInJava类（单词计数） 
 */
class WordCountTopo {

    public static void main(String[] args) throws Exception {
        // 创建一个拓扑
        TopologyBuilder builder = new TopologyBuilder();
        // 设置Spout，这个Spout的名字叫做"Spout"，设置并行度为5
        builder.setSpout("Spout", new RandomSentenceSpout(), 5);
        // 设置slot——“split”，并行度为8，它的数据来源是spout的
        builder.setBolt("split", new SplitSentenceBolt(), 8).shuffleGrouping("Spout");
        // 设置slot——“count”,你并行度为12，它的数据来源是split的word字段
        builder.setBolt("count", new WordCountBolt(), 12).fieldsGrouping("split", new Fields("word"));

        builder.setBolt("show", new ShowWordCountBolt(),3).globalGrouping("count");

        Config conf = new Config();
        conf.setDebug(false);

        // if(args != null && args.length > 0){
        // if(false){
        // conf.setNumWorkers(3);
        // StormSubmitter.submitTopology(args[0], conf,
        // builder.createTopology());
        // }else{
        conf.setMaxTaskParallelism(3);

        // 本地集群
        LocalCluster cluster = new LocalCluster();

        // 提交拓扑（该拓扑的名字叫word-count）
        cluster.submitTopology("word-count", conf, builder.createTopology());

        Utils.sleep(5000);
        cluster.killTopology("word-count");
        cluster.shutdown();
    }
}
