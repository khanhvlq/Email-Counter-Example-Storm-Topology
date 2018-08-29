import Bolt.EmailCounter;
import Bolt.EmailExtractor;
import Spout.CommitFeedListener;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class LocalTopologyRunner {
    public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("commit-feed-listener", new CommitFeedListener());
        builder.setBolt("email-extractor", new EmailExtractor()).shuffleGrouping("commit-feed-listener");
        builder.setBolt("email-counter", new EmailCounter()).shuffleGrouping("email-extractor");

        Config config = new Config();
        config.setDebug(true);

        StormTopology topology = builder.createTopology();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("count-email-topology", config, topology);

        Utils.sleep(60000);

        cluster.killTopology("count-email-topology");
        cluster.shutdown();

    }
}
