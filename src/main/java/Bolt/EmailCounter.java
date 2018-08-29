package Bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class EmailCounter extends BaseRichBolt {
    private Map<String, Integer> counts;
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        counts = new HashMap<String, Integer>();
    }

    public void execute(Tuple tuple) {
        System.out.println(tuple);
        String email = tuple.getStringByField("email");
        counts.put(email, countFor(email)+1);
        printCounts();
    }


    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    private Integer countFor(String email){
        Integer count = counts.get(email);
        return count == null ? 0 : count;
    }

    private void printCounts(){
        for (String email: counts.keySet()){
            System.out.println(email+": "+counts.get(email));
        }
    }
}
