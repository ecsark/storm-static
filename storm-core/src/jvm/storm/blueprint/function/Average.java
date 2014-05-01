package storm.blueprint.function;

import backtype.storm.tuple.Values;

import java.util.List;

/**
 * User: ecsark
 * Date: 4/21/14
 * Time: 1:32 PM
 */
public class Average implements Functional, Incremental {

    private double sum(List<List<Object>> input) {
        double s = 0.0;
        for (List<Object> obj : input) {
            double j = (Double) obj.get(0);
            s += j;
        }
        return s;
    }

    @Override
    public Values apply(List<List<Object>> input) {
        int numOfElements = input.size();
        double s = sum(input);

        return new Values(s/numOfElements, numOfElements);
    }

    @Override
    public Values update(List<List<Object>> newTuples, List<List<Object>> oldTuples, Values state) {
        double avg = (Double) state.get(0);

        int numOfElements = (Integer) state.get(1);

        double s = avg*numOfElements;
        s -= sum(oldTuples);
        s += sum(newTuples);

        return new Values(s/numOfElements, numOfElements);
    }
}
