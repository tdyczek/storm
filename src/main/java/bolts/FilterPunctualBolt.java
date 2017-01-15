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
 * Created by tom on 14.01.17.
 */
public class FilterPunctualBolt extends BaseRichBolt {
    OutputCollector _collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector=outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        float arrDelay = Float.parseFloat(tuple.getStringByField("ArrDel"));
        if(arrDelay <= 0){
            int dayOfWeek = Integer.parseInt(tuple.getStringByField("WeekDay"));
            _collector.emit(tuple, new Values(dayOfWeek));
        }
        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("DayOfWeek"));
    }
}
