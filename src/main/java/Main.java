import bolts.*;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
import spouts.DummySpout;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Created by tom on 13.01.17.
 */

public class Main {


    public static void main(String[] args) throws Exception {

        String zkIp = "127.0.0.1";
        String zookeeperHost = zkIp +":2181";

        ZkHosts zkHosts = new ZkHosts(zookeeperHost);

        SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, "storm2", "", "storm");

        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        KafkaSpout kafkaSpout = new KafkaSpout(kafkaConfig);
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("source", kafkaSpout, 2);
        builder.setBolt("split", new SplitBolt(), 2).shuffleGrouping("source");

        // ad1
        builder.setBolt("filterAirports", new FilterAirportsBolt(), 1).shuffleGrouping("split");
        builder.setBolt("cassandraAirportsWriter", new AirportCassandraBolt(),1).shuffleGrouping("filterAirports");
//
//        // ad2
        builder.setBolt("filterPunctual", new FilterPunctualBolt(), 1).shuffleGrouping("split");
        builder.setBolt("cassandraDaysWriter", new PunctualCassandraBolt(),1).shuffleGrouping("filterPunctual");
//
//        // ad3
        builder.setBolt("rankSplitted", new RankBolt(), 1).shuffleGrouping("split");
        builder.setBolt("cassandraRankWriter", new RankBoltCassandra(), 1).shuffleGrouping("rankSplitted");

        // ad4
        builder.setBolt("meanTime", new MeanTimeBolt(), 1).shuffleGrouping("split");
        builder.setBolt("cassandraMeanWriter", new MeanTimeCassandraBolt(), 1).shuffleGrouping("meanTime");


        Config conf = new Config();
        conf.setDebug(true);
//        conf.put(Config.TOPOLOGY_DEBUG, false);


        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());

    }
}