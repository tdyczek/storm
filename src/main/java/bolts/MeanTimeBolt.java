package bolts;

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
public class MeanTimeBolt extends BaseRichBolt{
    OutputCollector _collector;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String from = tuple.getStringByField("From");
        String to = tuple.getStringByField("To");
        int delay = Math.round(Float.parseFloat(tuple.getStringByField("ArrDel")));
        if(delay <= 0)
            delay = 0;
        _collector.emit(tuple, new Values(Integer.toString(delay), from, to));
        _collector.ack(tuple);


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("Delay", "From", "To"));
    }
}
