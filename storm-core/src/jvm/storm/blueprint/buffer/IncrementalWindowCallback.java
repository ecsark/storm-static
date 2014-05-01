package storm.blueprint.buffer;

import backtype.storm.tuple.Fields;

import java.io.Serializable;
import java.util.List;

/**
 * User: ecsark
 * Date: 4/20/14
 * Time: 11:59 AM
 */
public interface IncrementalWindowCallback extends Serializable{

    void process(List<List<Object>> newTuples, List<List<Object>> oldTuples);

    void initialize(List<List<Object>> initialTuples);

    Fields getInputFields();
}
