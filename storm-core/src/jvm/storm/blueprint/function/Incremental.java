package storm.blueprint.function;

import backtype.storm.tuple.Values;

import java.util.List;

/**
 * User: ecsark
 * Date: 4/21/14
 * Time: 6:08 PM
 */
public interface Incremental {

    Values update(List<List<Object>> newTuples, List<List<Object>> oldTuples, Values state);
}
