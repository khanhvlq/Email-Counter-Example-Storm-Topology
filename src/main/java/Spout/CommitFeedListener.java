package Spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class CommitFeedListener extends BaseRichSpout {

    private SpoutOutputCollector spoutOutputCollector;
    private List<String> commits;

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        String[] arr = { "b20ea50 nathan@example.com", "064874b andy@example.com", "28e4f8e andy@example.com", "9a3e07f andy@example.com", "cbb9cd1 nathan@example.com", "0f663d2 jackson@example.com", "0a4b984 nathan@example.com", "1915ca4 derek@example.com" };
        commits = Arrays.asList(arr);
    }

    public void nextTuple() {
        for(String commit: commits ){
            spoutOutputCollector.emit(new Values(commit));
            Utils.sleep(500);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("commit"));
    }
}
