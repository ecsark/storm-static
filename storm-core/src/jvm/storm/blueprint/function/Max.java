package storm.blueprint.function;

import backtype.storm.tuple.Values;

import java.util.List;

/**
 * User: ecsark
 * Date: 4/22/14
 * Time: 10:52 PM
 */
public class Max implements Functional {
    @Override
    public Values apply(List<List<Object>> input) {
        double max = Double.MIN_VALUE;

        for(List<Object> obj : input) {
            double num = (Double) obj.get(0);
            if (num > max)
                max = num;
        }

        return new Values(max);
    }

}
