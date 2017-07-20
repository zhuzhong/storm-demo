package com.z.storm.wordcount;

import java.util.StringTokenizer;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

// 定义个Bolt，用于将句子切分为单词  
class SplitSentenceBolt extends BaseBasicBolt {
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        // 接收到一个句子
        String sentence = tuple.getStringByField("sentence");
        // 把句子切割为单词
        StringTokenizer iter = new StringTokenizer(sentence);
        // 发送每一个单词
        while (iter.hasMoreElements()) {
            collector.emit(new Values(iter.nextToken()));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // 定义下一个字段
        declarer.declare(new Fields("word"));
    }
}