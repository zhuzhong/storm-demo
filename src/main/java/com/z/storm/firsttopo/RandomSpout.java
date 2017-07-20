package com.z.storm.firsttopo;

import java.util.Map;
import java.util.Random;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

 class RandomSpout extends BaseRichSpout {
    /**
	 * 
	 */
    private static final long serialVersionUID = 3504927994092707306L;
    private SpoutOutputCollector collector;
    private static String[] words = { "happy", "excited", "angry" };

    @Override
    public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
        this.collector = arg2;
    }

    @Override
    public void nextTuple() {
        String word = words[new Random().nextInt(words.length)];
        collector.emit(new Values(word));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer arg0) {
        arg0.declare(new Fields("randomstring"));
    }
}