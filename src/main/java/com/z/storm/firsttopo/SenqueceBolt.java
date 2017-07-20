package com.z.storm.firsttopo;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

class SenqueceBolt extends BaseBasicBolt {
    /**
     * 
     */
    private static final long serialVersionUID = 7624985770369970364L;

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        // TODO Auto-generated method stub
        String word = (String) input.getValue(0);
        String out = "I'm " + word + "!";
        System.out.println("out=" + out);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}