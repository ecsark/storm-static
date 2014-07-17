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

    transient Map<String, List<UseLink>> partitions;
    transient Map<String, LibreBuffer> buffers;


    public Collection<LibreBuffer> build (List<UseLink> links, Collection<WindowItem> windows,
                                          Functional function,
                                          Fields selectField,
                                          final AutoBolt bolt) {

        Map<String, List<ResultDependency>> dependencies = resolveDependencies(links);

        Set<String> nonEmittingWindows = new HashSet<String>();
        for (WindowItem window : windows) {
            if (!window.isEmitting())
                nonEmittingWindows.add(window.id);
        }

        Set<String> redundantWindowIds = removeRedundantDependencies(dependencies, nonEmittingWindows, links);

        extractPartition(links);

        buffers = new HashMap<String, LibreBuffer>();

        for (WindowItem window : windows) {

            if (redundantWindowIds.contains(window.id))
                continue;

            List<UseLink> partition = partitions.get(window.id);

            LibreBuffer buffer = new LibreBuffer(window.id, partition.size(),
                    computeLayerNum(window, partition), window.pace, window.length);

            buffer.setAncestorStates(computeAncestorStates(partition, window.pace, window.length));
            buffer.setEmitting(window.isEmitting());
            buffer.setSelectFields(selectField);
            buffers.put(window.id, buffer);
        }


        // for each buffer
        for (final LibreBuffer buffer : buffers.values()) {

            List<ResultDependency> dependencyList = dependencies.get(buffer.getId());

            // add final aggregation result declare
            if (dependencyList!=null && dependencyList.size()>0) {
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
                        final int trigger = counterEntry.getKey();

                        callbacks.add(new WindowResultCallback() {
                            int step = 0;
                            @Override
                            public void process(Tuple tuple) {
                                if (step == trigger)
                                    buf.put(tuple, destComponentIndices);
                                step = (step +1) % cycle;
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
                            //ResultWriter.write(bolt.getId(), bufferId+": "+tuple.getValues().get(0)+"\n");
                        }
                    });
                }

                AggregationStrategy strategy = new AggregationStrategy(function, aggStep.step, callbacks);
                buffer.addAggregationStrategy(strategy);

            }
        }

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

        partitions = new HashMap<String, List<UseLink>>();

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


    /*
     * @return Redundant window IDs
     */
    private Set<String> removeRedundantDependencies (Map<String, List<ResultDependency>> dependencies,
                                              Set<String> nonEmittingWindows, List<UseLink> allLinks) {


        Set<String> allRemovedWindows = new HashSet<String>();

        Set<String> unusedWindows = new HashSet<String>();
        for (String windowId : nonEmittingWindows) {
            if (!dependencies.containsKey(windowId))
                unusedWindows.add(windowId);
        }
        allRemovedWindows.addAll(unusedWindows);

        while (unusedWindows.size() > 0) {

            Set<String> windowsToRemove = new HashSet<String>();

            for (String windowId : dependencies.keySet()) {

                List<ResultDependency> dependencyList = dependencies.get(windowId);

                Iterator<ResultDependency> depIter = dependencyList.iterator();
                while (depIter.hasNext()) {

                    ResultDependency dependency = depIter.next();

                    Iterator<UseLink> linkIter = dependency.dependents.iterator();
                    while (linkIter.hasNext()) {
                        UseLink link = linkIter.next();
                        if (unusedWindows.contains(link.dest))
                            linkIter.remove();
                    }

                    if (dependency.dependents.size() == 0)
                        depIter.remove();
                }

                if (dependencyList.size() == 0 && nonEmittingWindows.contains(windowId))
                    windowsToRemove.add(windowId);
            }

            for (String windowId : windowsToRemove)
                dependencies.remove(windowId);

            allRemovedWindows.addAll(windowsToRemove);

            unusedWindows = windowsToRemove;
        }

        Iterator<UseLink> linkIter = allLinks.iterator();
        while(linkIter.hasNext()) {
            UseLink link = linkIter.next();
            if (allRemovedWindows.contains(link.component.windowId)
                    || allRemovedWindows.contains(link.dest))
                linkIter.remove();
        }


        return allRemovedWindows;
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
                                                           List<UseLink> partitioning) {

        // dependencies should be sorted in the ascending order of <finish time, length>
        // which should be done already

        // partitioning should be sorted in the ascending order of finish time
        // which should be done already

        List<AggregationDependency> steps = new ArrayList<AggregationDependency>();

        Iterator<ResultDependency> decIter = dependencies.iterator();

        if (decIter.hasNext()) {

            ResultDependency dec = decIter.next();

            for (int i=0; i<partitioning.size(); ++i) {
                UseLink partition = partitioning.get(i);

                while (partition.start+partition.component.length == dec.declaration.start+dec.declaration.length) {
                    for (int j=i; j>=0; --j) {
                        UseLink firstPartition = partitioning.get(j);
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

        // partition should be sorted in the ascending order of finish time
        // which should be done already

        return (window.length - partition.get(0).component.length) / window.pace + 1;
    }

    protected List<Integer> computeAncestorStates(List<UseLink> partition, int pace, int windowLength) {

        int nAncestor = (windowLength-1) / pace;

        List<Integer> states = new ArrayList<Integer>(nAncestor);
        int length = 0;

        for (int i=1, j=0; i<=nAncestor; ++i) {
            // so that l_0+l_1+...+l_{j-1} >= i*pace
            while (length < i*pace) {
                length += partition.get(j).component.length;
                ++j;
            }
            states.add(j);
        }

        int nPartitions = partition.size();
        List<Integer> statesReversed = new ArrayList<Integer>();
        for (int i=states.size()-1; i>=0 ; --i) {
            if (states.get(i)!=nPartitions)
                statesReversed.add(states.get(i));
        }

        return statesReversed;
    }


    // <triggerCounter, List<destComponentIds>>
    protected Map<Integer, List<Integer>> computeForwardingPattern (List<UseLink> links) {

        if (links.size() == 0) {
            return new HashMap<Integer, List<Integer>>();
        }

        // see if there is coincidence
        final int rotation = links.get(0).pace/links.get(0).component.pace; // there should be at least one element!!
        ListMap<Integer, UseLink> counterMap = new ListMap<Integer, UseLink>(links,
            new ListMap.KeyExtractable<Integer, UseLink>() {
                @Override
                public Integer getKey(UseLink item) {
                    return ((item.start-item.component.start)%item.pace)/item.component.pace;
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

