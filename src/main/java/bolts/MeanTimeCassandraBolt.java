package bolts;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by tom on 13.01.17.
 */
public class MeanTimeCassandraBolt extends BaseRichBolt{
    OutputCollector _collector;
    Cluster cluster = null;
    Session session = null;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
            cluster = Cluster.builder()                                                    // (1)
                    .addContactPoint("127.0.0.1")
                    .build();
            session = cluster.connect();                                           // (2)


    }

    @Override
    public void execute(Tuple tuple) {
        String from = tuple.getStringByField("From");
        String to = tuple.getStringByField("To");
        String delay = tuple.getStringByField("Delay");

        session.execute("UPDATE tutorialspoint.mean_arrival " +
                           "SET number_of_flights=number_of_flights+1, sum_of_delays=sum_of_delays + " + delay + " " +
                           "WHERE source='" + from + "' AND dest='" +to+ "';");


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("Delay", "From", "To"));
    }
}
