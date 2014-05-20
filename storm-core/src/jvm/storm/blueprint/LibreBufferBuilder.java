package storm.blueprint;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.blueprint.buffer.AggregationStep;
import storm.blueprint.buffer.AggregationStrategy;
import storm.blueprint.buffer.LibreTupleBuffer;
import storm.blueprint.buffer.LibreWindowCallback;
import storm.blueprint.function.Functional;

import java.io.Serializable;
import java.util.*;

/**
 * User: ecsark
 * Date: 5/14/14
 * Time: 3:29 PM
 */
public class LibreBufferBuilder implements Serializable {

    public Collection<LibreTupleBuffer> build (PaceGroup paceGroup, Functional function, Fields selectField,
                                               final LibreBolt bolt) {

        Map<String, List<ResultDependency>> dependencies = resolveDependencies(paceGroup.links);
        Map<String, List<UseLink>> partitions = extractPartition(paceGroup.links);
        Map<String, LibreTupleBuffer> buffers = new HashMap<String, LibreTupleBuffer>();

        for (WindowItem window : paceGroup.windows) {

            LibreTupleBuffer buffer;

            List<UseLink> partition = partitions.get(window.id);
            buffer = new LibreTupleBuffer(window.id, partition.size(),
                    calculateLayerNum(window, partition), window.pace, window.windowLength);

            buffer.setAncestorStates(calculateAncestorStates(partition,window.pace, window.windowLength));
            buffer.setEmitting(window.isEmitting());
            buffer.setSelectFields(selectField);
            buffers.put(window.id, buffer);
        }

        // for each buffer
        for (LibreTupleBuffer buffer : buffers.values()) {

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
                List<LibreWindowCallback> callbacks = new ArrayList<LibreWindowCallback>();

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

                    //TODO: different frequency!!!
                    final LibreTupleBuffer buf = buffers.get(linkEntry.getKey());
                    final List<Integer> destComponentIndices = new ArrayList<Integer>();

                    // extract component index from uselink to a list
                    // and sort them in the descending order
                    for (UseLink link : linkEntry.getValue()) {
                        destComponentIndices.add(link.index);
                    }
                    Collections.sort(destComponentIndices, new Comparator<Integer>() {
                        @Override
                        public int compare(Integer o1, Integer o2) {
                            return o2-o1;
                        }
                    });

                    callbacks.add(new LibreWindowCallback() {
                        @Override
                        public void process(Tuple tuples) {
                            buf.put(tuples, destComponentIndices);
                        }
                    });
                }

                // for the final aggregation
                if (buffer.isEmitting() && aggStep.dependencies.declaration.start == 0
                        && aggStep.dependencies.declaration.length==buffer.getLength()) {

                    final String bufferId = buffer.getId();

                    callbacks.add(new LibreWindowCallback() {
                        @Override
                        public void process(Tuple tuples) {
                            bolt.getCollector().emit(bufferId, new Values(tuples.getValues().get(0)));
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
            ResultDeclaration part = link.part;

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

                while (partition.start+partition.part.length == dec.declaration.start+dec.declaration.length) {
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


    protected int calculateLayerNum(WindowItem window, List<UseLink> partition) {

        // partitions should be sorted in the ascending order of finish time
        // which should be done already

        return (window.windowLength - partition.get(0).part.length) / window.pace + 1;
    }

    protected List<Integer> calculateAncestorStates (List<UseLink> partition, int pace, int windowLength) {

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
                length += partition.get(j).part.length;
                ++j;
            }

            states.set(nAncestor-i, j);
        }

        return states;

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
            assert(dependent.part==declaration); // TODO: remove this line
            if (dependent.part!=declaration)
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

