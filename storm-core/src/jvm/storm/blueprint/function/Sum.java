package storm.blueprint.function;

import backtype.storm.tuple.Values;
import storm.blueprint.util.Counter;

import java.util.List;

/**
 * User: ecsark
 * Date: 4/22/14
 * Time: 10:52 PM
 */
public class Sum implements Functional {

    Counter counter;

    public Sum setCounter (Counter counter) {
        this.counter = counter;
        return this;
    }

    @Override
    public Values apply(List<List<Object>> input) {
        double s = 0;

        if (counter!=null)
            counter.increment(input.size()-1);

        for(List<Object> obj : input) {
            s += (Double) obj.get(0);
        }

        return new Values(s);
    }

}
