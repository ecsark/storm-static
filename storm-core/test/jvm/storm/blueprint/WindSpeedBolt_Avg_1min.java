package storm.blueprint;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.blueprint.buffer.IncrementalSlidingWindowBuffer;
import storm.blueprint.buffer.IncrementalWindowCallback;
import storm.blueprint.function.Average;

import java.util.List;
import java.util.Map;

/**
 * User: ecsark
 * Date: 4/21/14
 * Time: 2:13 PM
 */
public class WindSpeedBolt_Avg_1min extends BaseBasicBolt {

    IncrementalSlidingWindowBuffer buffer;
    Average func;

    Values state;

    BasicOutputCollector _collector;

    @Override
    public void prepare(Map stormConf, final TopologyContext context) {

        buffer = new IncrementalSlidingWindowBuffer(60,5);
        func = new Average();


        buffer.setCallback(new IncrementalWindowCallback() {
            @Override
            public void process(List<List<Object>> newTuples, List<List<Object>> oldTuples) {
                state = func.update(newTuples, oldTuples, state);
                _collector.emit(new Values(state.get(0)));
            }

            @Override
            public void initialize(List<List<Object>> initialTuples) {
                state = func.apply(initialTuples);
            }

            @Override
            public Fields getInputFields() {
                return new Fields("windspeed");
            }
        });
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        this._collector = collector;
        buffer.put(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("windspeed_avg_1min"));
    }
}
