import bolts.*;
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

    static IRichBolt cassandraAirportWriter = new CassandraWriterBolt(
            async(
                    simpleQuery("UPDATE airports SET number = number + 1 WHERE airport_name=?;")
                    .with(all())));

    static IRichBolt cassandraDaysWriter = new CassandraWriterBolt(
            async(
                    simpleQuery("UPDATE days_of_week SET no_of_punctual = no_of_punctual + 1 WHERE day=?;")
                            .with(all())));


    static IRichBolt cassandraRankWriter = new CassandraWriterBolt(
            async(
                    simpleQuery("UPDATE popular_airports SET number_of_flights=number_of_flights+1 WHERE source =?AND carrier =?;")
                            .with(fields( "Airport", "Carrier"))
            ));


    static IRichBolt cassandraMeanWriter = new CassandraWriterBolt(
            async(
                    simpleQuery("UPDATE mean_arrival SET number_of_flights=number_of_flights+1, sum_of_delays=sum_of_delays WHERE source =?AND dest =?;")
                            .with(fields("Delay", "From", "To")))
    );


    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("source", new DummySpout(), 1);
        builder.setBolt("split", new SplitBolt(), 1).shuffleGrouping("source");

        // ad1
        builder.setBolt("filterAirports", new FilterAirportsBolt(), 1).shuffleGrouping("split");
        builder.setBolt("cassandraAirportsWriter", cassandraAirportWriter,1).shuffleGrouping("filterAirports");

        // ad2
        builder.setBolt("filterPunctual", new FilterPunctualBolt(), 1).shuffleGrouping("split");
        builder.setBolt("cassandraDaysWriter", cassandraDaysWriter,1).shuffleGrouping("filterPunctual");

        // ad3
        builder.setBolt("rankSplitted", new RankBolt(), 1).shuffleGrouping("split");
        builder.setBolt("cassandraRankWriter", cassandraRankWriter, 1).shuffleGrouping("rankSplitted");

        // ad4
        builder.setBolt("meanTime", new MeanTimeBolt(), 1).shuffleGrouping("split");
        builder.setBolt("cassandraMeanWriter", cassandraMeanWriter, 1).shuffleGrouping("meanTime");



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