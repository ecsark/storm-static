package storm.blueprint.function;

import backtype.storm.tuple.Values;

import java.util.List;

import static java.lang.Math.sqrt;

/**
 * User: ecsark
 * Date: 4/22/14
 * Time: 10:47 AM
 */
public class StandardDeviation implements Functional, Incremental {

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
        double mean = sum(input)/numOfElements;

        double s = 0;
        for(List<Object> obj : input) {
            double delta = (Double) obj.get(0) - mean;
            s += delta * delta;
        }

        double dev = s/(numOfElements-1);

        return new Values(sqrt(dev), numOfElements, mean, dev);
    }

    @Override
    public Values update(List<List<Object>> newTuples, List<List<Object>> oldTuples, Values state) {

        int numOfElements = (Integer) state.get(1);
        double mean = (Double) state.get(2);
        double m2 = (Double) state.get(3);

        for (int i=0; i<newTuples.size(); ++i) {
            double newX = (Double) newTuples.get(i).get(0);
            double oldX = (Double) oldTuples.get(i).get(0);

            double delta = newX - oldX;
            double dold = oldX - mean;
            mean += delta/numOfElements;
            double dnew = newX - mean;
            m2 += delta * (dold + dnew);
        }

        double dev = m2/(numOfElements-1);

        return new Values(sqrt(dev), numOfElements, mean, dev);
    }
}
