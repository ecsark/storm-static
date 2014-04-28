package storm.blueprint.function;

import backtype.storm.tuple.Values;

import java.util.List;

import static java.lang.Math.sqrt;

/**
 * User: ecsark
 * Date: 4/21/14
 * Time: 1:49 PM
 */
public class RootMeanSquare implements Functional, Incremental {

    private double squareSum(List<List<Object>> input) {
        double s = 0.0;
        for (List<Object> obj : input) {
            double j = (Double) obj.get(0);
            s += j*j;
        }
        return s;
    }

    @Override
    public Values apply(List<List<Object>> input) {
        int numOfElements = input.size();
        double s = squareSum(input);
        double t = s/numOfElements;

        return new Values(sqrt(t), numOfElements, s);
    }

    @Override
    public Values update(List<List<Object>> newTuples, List<List<Object>> oldTuples, Values state) {

        int numOfElements = (Integer) state.get(1);
        double s = (Double) state.get(2);

        s -= squareSum(oldTuples);
        s += squareSum(newTuples);
        double t = s/numOfElements;

        return new Values(sqrt(t), numOfElements, s);
    }
}
