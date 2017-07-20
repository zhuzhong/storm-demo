package com.z.storm.wordcount;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

public class ShowWordCountBolt extends BaseBasicBolt {
    Map<String, Integer> counts = new ConcurrentHashMap<String, Integer>();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        // 接收一个单词
        String word = tuple.getStringByField("word");
        Integer count = tuple.getIntegerByField("count");

        // System.out.println(String.format("ShowCount print %s--%d", word,
        // count));
        counts.put(word, count);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void cleanup() {
        for (Map.Entry<String, Integer> entry : counts.entrySet()) {
            System.out.println(String.format("ShowCount print %s--%d", entry.getKey(), entry.getValue()));
        }
    }
}