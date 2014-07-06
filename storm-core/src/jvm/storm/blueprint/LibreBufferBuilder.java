package storm.blueprint;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.blueprint.buffer.AggregationStep;
import storm.blueprint.buffer.AggregationStrategy;
import storm.blueprint.buffer.LibreBuffer;
import storm.blueprint.buffer.WindowResultCallback;
import storm.blueprint.function.Functional;
import storm.blueprint.util.ListMap;

import java.io.Serializable;
import java.util.*;

/**
 * User: ecsark
 * Date: 5/14/14
 * Time: 3:29 PM
 */
public class LibreBufferBuilder implements Serializable {

    public Collection<LibreBuffer> build (List<UseLink> links, Collection<WindowItem> windows,
                                          Functional function,
                                          Fields selectField,
                                          final AutoBolt bolt) {

        Map<String, List<ResultDependency>> dependencies = resolveDependencies(links);
        Map<String, List<UseLink>> partitions = extractPartition(links);
        Map<String, LibreBuffer> buffers = new HashMap<String, LibreBuffer>();

        for (WindowItem window : windows) {

            LibreBuffer buffer;

            List<UseLink> partition = partitions.get(window.id);
            buffer = new LibreBuffer(window.id, partition.size(),
                    computeLayerNum(window, partition), window.pace, window.length);

            buffer.setAncestorStates(computeAncestorStates(partition, window.pace, window.length));
            buffer.setEmitting(window.isEmitting());
            buffer.setSelectFields(selectField);
            buffers.put(window.id, buffer);
        }

        // for each buffer
        for (LibreBuffer buffer : buffers.values()) {

            List<ResultDependency> dependencyList = dependencies.get(buffer.getId());

            // add final aggregation result declare
            if (dependencyList!=null) {
                ResultDependency lastResult = dependencyList.get(dependencyList.size() - 1);
                if (lastResult.declaration.start + lastResult.declaration.length != buffer.getLength()) {
                    dependencyList.add(new ResultDependency(
                            new ResultDeclaration(buffer.getLength(), 0, buffer.getPace(), buffer.getId())));
                }
            } else { // in case there is no result reuse of this window at all
                dependencyList = new ArrayList<ResultDependency>();
                dependencyList.add(new ResultDependency(
                        new ResultDeclaration(buffer.getLength(), 0, buffer.getPace(), buffer.getId())));
            }

            List<AggregationDependency> aggregationSteps = generateAggregationStep(dependencyList, partitions.get(buffer.getId()));

            // for each aggregation step
            for (AggregationDependency aggStep : aggregationSteps) {
                List<WindowResultCallback> callbacks = new ArrayList<WindowResultCallback>();

                // group by dest id
                ListMap<String, UseLink> linkGroup = new ListMap<String, UseLink>(
                        aggStep.dependencies.dependents,
                        new ListMap.KeyExtractable<String, UseLink>() {
                            @Override
                            public String getKey(UseLink item) {
                                return item.dest;
                            }
                        });

                // for each result receiver
                for (Map.Entry<String, List<UseLink>> linkEntry : linkGroup.map.entrySet()) {

                    final LibreBuffer buf = buffers.get(linkEntry.getKey());

                    Map<Integer, List<Integer>> counterMap = computeForwardingPattern(linkEntry.getValue());

                    UseLink sample = linkEntry.getValue().get(0); // there should be at least one element!
                    final int cycle = sample.pace / sample.component.pace;

                    for (Map.Entry<Integer, List<Integer>> counterEntry : counterMap.entrySet()) {

                        final List<Integer> destComponentIndices = counterEntry.getValue();
                        final int triggerCounter = counterEntry.getKey();

                        callbacks.add(new WindowResultCallback() {
                            int counter = 0;
                            @Override
                            public void process(Tuple tuple) {
                                if (counter == triggerCounter)
                                    buf.put(tuple, destComponentIndices);
                                counter = (counter+1) % cycle;
                            }
                        });
                    }
                }

                // for the final aggregation
                if (buffer.isEmitting() && aggStep.dependencies.declaration.start == 0
                        && aggStep.dependencies.declaration.length==buffer.getLength()) {

                    final String bufferId = buffer.getId();

                    callbacks.add(new WindowResultCallback() {
                        @Override
                        public void process(Tuple tuple) {
                            bolt.getCollector().emit(bufferId, new Values(tuple.getValues().get(0)));
                        }
                    });
                }

                AggregationStrategy strategy = new AggregationStrategy(function, aggStep.step, callbacks);
                buffer.addAggregationStrategy(strategy);

            }

        }

        // TODO: set entrance?

        return buffers.values();
    }

    /*
        Pick up those ResultDeclaration that were actually utilized, grouped by referenced window id
        windId -> declaration -> dependencyLink
     */
    private Map<String, List<ResultDependency>> resolveDependencies (List<UseLink> links) {
        Map<String, List<ResultDependency>> dependencies = new HashMap<String, List<ResultDependency>>();

        for (UseLink link : links) {
            ResultDeclaration part = link.component;

            if (!dependencies.containsKey(part.windowId))
                dependencies.put(part.windowId, new ArrayList<ResultDependency>());

            List<ResultDependency> declarations = dependencies.get(part.windowId);

            boolean exist = false;
            for (ResultDependency relation : declarations) {
                if (relation.declaration == part) {
                    relation.addTarget(link);
                    exist = true;
                }
            }

            if (!exist) {
                declarations.add(new ResultDependency(part, link));
            }
        }


        // sort dependencies in the ascending order of <finish time, length>
        for (List<ResultDependency> dependencyList : dependencies.values()) {
            Collections.sort(dependencyList, new Comparator<ResultDependency>() {
                @Override
                public int compare(ResultDependency r1, ResultDependency r2) {
                    ResultDeclaration o1 = r1.declaration, o2 = r2.declaration;

                    if (o1.start+o1.length == o2.start+o2.length)
                        return o1.length - o2.length;
                    else
                        return (o1.start+o1.length - (o2.start+o2.length));
                }
            });
        }

        return dependencies;
    }

    private Map<String, List<UseLink>> extractPartition (List<UseLink> links) {
        Map<String, List<UseLink>> partitions = new HashMap<String, List<UseLink>>();

        // group by receiver
        for (UseLink link : links) {

            if (!partitions.containsKey(link.dest))
                partitions.put(link.dest, new ArrayList<UseLink>());

            partitions.get(link.dest).add(link);
        }

        for (List<UseLink> partition : partitions.values()) {
            // sort partitions in the ascending order of finish time
            Collections.sort(partition, new Comparator<UseLink>() {
                @Override
                public int compare(UseLink o1, UseLink o2) {
                    return o1.start - o2.start;
                }
            });

            // set component indices
            for (int i=0; i<partition.size(); ++i) {
                partition.get(i).index = i;
            }
        }

        return partitions;
    }


    private List<Integer> quickList (int from, int to) {
        List<Integer> list = new ArrayList<Integer>();
        for (int i=from; i<=to; ++i) {
            list.add(i);
        }
        return list;
    }

    /*
        Generate the aggregation step of a window.
        Goal: reducing the number of steps
     */
    private List<AggregationDependency> generateAggregationStep (List<ResultDependency> dependencies,
                                                           List<UseLink> partitions) {

        // dependencies should be sorted in the ascending order of <finish time, length>
        // which should be done already

        // partitions should be sorted in the ascending order of finish time
        // which should be done already

        List<AggregationDependency> steps = new ArrayList<AggregationDependency>();

        Iterator<ResultDependency> decIter = dependencies.iterator();

        if (decIter.hasNext()) {

            ResultDependency dec = decIter.next();

            for (int i=0; i<partitions.size(); ++i) {
                UseLink partition = partitions.get(i);

                while (partition.start+partition.component.length == dec.declaration.start+dec.declaration.length) {
                    for (int j=i; j>=0; --j) {
                        UseLink firstPartition = partitions.get(j);
                        if (firstPartition.start == dec.declaration.start) {
                            steps.add(new AggregationDependency(new AggregationStep(quickList(j,i), j, i), dec));
                            break;
                        }
                    }

                    if (decIter.hasNext()) {
                        dec = decIter.next();
                    } else {
                        break;
                    }
                }
            }
        }

        // remove redundancy
        for (int i=0; i<steps.size(); ++i) {
            List<Integer> inputPositions = steps.get(i).step.getInputPositions();

            for (int j=i+1; j<steps.size(); ++j) {
                List<Integer> positions = steps.get(j).step.getInputPositions();

                for (int k=1; k < inputPositions.size(); ++k)
                    positions.remove((Integer) inputPositions.get(k));
            }
        }

        return steps;
    }


    protected int computeLayerNum(WindowItem window, List<UseLink> partition) {

        // partitions should be sorted in the ascending order of finish time
        // which should be done already

        return (window.length - partition.get(0).component.length) / window.pace + 1;
    }

    protected List<Integer> computeAncestorStates(List<UseLink> partition, int pace, int windowLength) {


        int nAncestor = (windowLength-1) / pace;

        List<Integer> states = new ArrayList<Integer>(nAncestor);
        // pre-stuffing!
        for (int i=0; i<nAncestor; ++i) {
            states.add(0);
        }

        int length = 0;

        for (int i=1, j=0; i<=nAncestor; ++i) {
            // so that l_0+l_1+...+l_{j-1} >= i*pace
            while (length < i*pace) {
                length += partition.get(j).component.length;
                ++j;
            }
            states.set(nAncestor-i, j);
        }

        return states;
    }


    // <triggerCounter, List<destComponentIds>>
    protected Map<Integer, List<Integer>> computeForwardingPattern (List<UseLink> links) {

        if (links.size() == 0) {
            return new HashMap<Integer, List<Integer>>();
        }

        List<Integer> counters = new ArrayList<Integer>();
        for (UseLink link : links) {
            int counter = link.start/link.component.pace;
            counters.add(counter);
        }
        // see if there is coincidence
        final int rotation = links.get(0).pace/links.get(0).component.pace; // there should be at least one element!!
        ListMap<Integer, UseLink> counterMap = new ListMap<Integer, UseLink>(links,
            new ListMap.KeyExtractable<Integer, UseLink>() {
                @Override
                public Integer getKey(UseLink item) {
                    return (item.start/item.component.pace) % rotation;
                }
            });

        // sort each list in the descending order of index
        for(List<UseLink> counterList : counterMap.map.values()) {
            Collections.sort(counterList, new Comparator<UseLink>() {
                @Override
                public int compare(UseLink o1, UseLink o2) {
                    return o2.index - o1.index;
                }
            });
        }

        // extract the indices from UseLink
        Map<Integer, List<Integer>> map = new HashMap<Integer, List<Integer>>();
        for (Map.Entry<Integer, List<UseLink>> entry : counterMap.map.entrySet()) {
            List<Integer> list = new ArrayList<Integer>();
            for (UseLink link : entry.getValue())
                list.add(link.index);
            map.put(entry.getKey(), list);
        }

        return map;
    }


    class ResultDependency {
        ResultDeclaration declaration;
        List<UseLink> dependents;

        ResultDependency(ResultDeclaration declaration) {
            this.declaration = declaration;
            dependents = new ArrayList<UseLink>();
        }

        ResultDependency(ResultDeclaration declaration, UseLink dependent) {
            this(declaration);
            addTarget(dependent);
        }


        boolean addTarget (UseLink dependent) {
            assert(dependent.component ==declaration); // TODO: remove this line
            if (dependent.component !=declaration)
                return false;
            dependents.add(dependent);
            return true;
        }
    }

    class AggregationDependency {
        AggregationStep step;
        ResultDependency dependencies;

        AggregationDependency(AggregationStep step, ResultDependency dependencies) {
            this.step = step;
            this.dependencies = dependencies;
        }
    }

}

