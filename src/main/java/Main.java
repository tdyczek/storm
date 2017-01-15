import bolts.*;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import spouts.DummySpout;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by tom on 13.01.17.
 */

public class Main {

//    static IRichBolt cassandraAirportWriter = new CassandraWriterBolt(
//            async(
//                    simpleQuery("UPDATE airports SET number = number + 1 WHERE airport_name=?;")
//                    .with(all())));
//



    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("source", new DummySpout(), 1);
        builder.setBolt("split", new SplitBolt(), 1).shuffleGrouping("source");

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


        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());
        Utils.sleep(10000);
        cluster.killTopology("test");
        cluster.shutdown();
    }
}