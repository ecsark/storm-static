package storm.blueprint.function;

import backtype.storm.tuple.Values;

import java.util.List;

/**
 * User: ecsark
 * Date: 4/21/14
 * Time: 6:07 PM
 */
public interface Functional {

    Values apply(List<List<Object>> input);
}
