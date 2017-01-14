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
public class SplitBolt extends BaseRichBolt{
    OutputCollector _collector;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String[] splittedLine = tuple.getString(0).split(",");
        _collector.emit(tuple, new Values(
                splittedLine[4],
                splittedLine[17],
                splittedLine[18],
                splittedLine[9],
                splittedLine[15],
                splittedLine[16]
                ));
        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("WeekDay", "From", "To", "LineName", "ArrDel", "DepDelay"));
    }
}
