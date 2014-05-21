package storm.blueprint.function;

import backtype.storm.tuple.Values;

import java.util.List;

/**
 * User: ecsark
 * Date: 4/22/14
 * Time: 10:52 PM
 */
public class Sum implements Functional {
    @Override
    public Values apply(List<List<Object>> input) {
        double s = 0;

        for(List<Object> obj : input) {
            s += (Double) obj.get(0);
        }

        return new Values(s);
    }

}
