package storm.blueprint;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.blueprint.buffer.TupleBuffer;
import storm.blueprint.function.Functional;

/**
 * User: ecsark
 * Date: 4/21/14
 * Time: 2:13 PM
 */
public class AutoBolt extends BaseBasicBolt {

    TupleBuffer buffer;

    TupleBuffer cacheBuffer;

    Functional function;
    Fields outputFields;
    Fields inputFields;
    String name;

    Values state;

    BasicOutputCollector _collector;

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        this._collector = collector;
        buffer.put(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(outputFields);
    }
}
