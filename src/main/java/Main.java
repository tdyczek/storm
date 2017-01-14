import bolts.FilterAirportsBolt;
import bolts.SplitBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.cassandra.bolt.CassandraWriterBolt;
import org.apache.storm.task.IBolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import spouts.DummySpout;


import static org.apache.storm.cassandra.DynamicStatementBuilder.*;

/**
 * Created by tom on 13.01.17.
 */

public class Main {

    static IRichBolt cassandraWriter = new CassandraWriterBolt(
            async(
                    simpleQuery("UPDATE airports SET number = number + 1 WHERE source=? AND dest=?")
                    .with(all())));



    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("source", new DummySpout(), 1);
        builder.setBolt("split", new SplitBolt(), 1).shuffleGrouping("source");
        builder.setBolt("filterAirports", new FilterAirportsBolt(), 1).shuffleGrouping("split");
        builder.setBolt("cassandraAirportsWriter", cassandraWriter,1).shuffleGrouping("filterAirports");
        Config conf = new Config();
        conf.setDebug(true);
        conf.put("cassandra.keyspace", "tutorialspoint");


        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());
        Utils.sleep(10000);
        cluster.killTopology("test");
        cluster.shutdown();
    }
}