package storm.blueprint;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import storm.blueprint.buffer.TupleBuffer;

import java.io.Serializable;
import java.util.Map;

/**
 * User: ecsark
 * Date: 4/28/14
 * Time: 2:08 PM
 */
public class AutoBoltPro extends BaseBasicBolt implements Serializable {

    Map<String, BufferStack> stack;

    String boltName;

    Fields inputFields;

    Fields outputFields;

    BasicOutputCollector collector;

    TupleBuffer entrance;


    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        if (stack.size() < 1) {
            throw new RuntimeException("Buffer stack should not be empty!");
        }
        if (entrance == null) {
            throw new RuntimeException("Tuple entrance has not been set!");
        }
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        this.collector = collector;
        entrance.put(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        for (BufferStack s : stack.values()) {
            declarer.declareStream(s.outputStreamId, outputFields);
        }
    }
}
