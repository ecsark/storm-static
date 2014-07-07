package storm.blueprint;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import storm.blueprint.buffer.ColdBuffer;
import storm.blueprint.buffer.LibreBuffer;
import storm.blueprint.buffer.WindowResultCallback;
import storm.blueprint.function.Functional;
import storm.blueprint.util.ListMap;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * User: ecsark
 * Date: 7/7/14
 * Time: 11:51 AM
 */
public class ColdBufferBuilder implements Serializable {

    private class ColdComponent implements Serializable{
        UseLink part;
        int suppressCount;
        int transferCount;

        ColdComponent(UseLink part, int suppressCount, int transferCount) {
            this.part = part;
            this.suppressCount = suppressCount;
            this.transferCount = transferCount;
        }
    }


    private List<ColdComponent> getColdComponents (List<UseLink> partition) {
        List<ColdComponent> components = new ArrayList<ColdComponent>();

        for (UseLink part : partition) {
            int transferCount = 0, suppressCount = 0;

            int start = part.start;
            while (start >= 0) {
                if (start < part.component.start)
                    transferCount++;
                else
                    suppressCount++;
                start -= part.pace;
            }

            start = part.start + part.pace;
            while (start < part.component.start) {
                transferCount++;
                start += part.pace;
            }

            if (transferCount > 0)
                components.add(new ColdComponent(part, suppressCount, transferCount));
        }

        return components;
    }

    private List<ColdBuffer> buildColdBuffers (List<ColdComponent> allColdComponents,
                                               Map<String, LibreBuffer> buffers,
                                               Functional function,
                                               Fields selectField) {

        ListMap<ResultDeclaration, ColdComponent> declarationGroup = new ListMap<ResultDeclaration, ColdComponent>(
                allColdComponents, new ListMap.KeyExtractable<ResultDeclaration, ColdComponent>() {
            @Override
            public ResultDeclaration getKey(ColdComponent item) {
                return item.part.component;
            }
        });

        List<ColdBuffer> coldBuffers = new ArrayList<ColdBuffer>();

        for (List<ColdComponent> components : declarationGroup.getMap().values()) {
            ResultDeclaration declaration = components.get(0).part.component;

            int start = declaration.start % declaration.pace;
            int size = start + declaration.length;
            ColdBuffer buffer = new ColdBuffer("__cold"+declaration.toString(), size, declaration.pace, start);
            buffer.setFunction(function);
            buffer.setSelectFields(selectField);
            coldBuffers.add(buffer);

            for (final ColdComponent component : components) {

                final LibreBuffer buf = buffers.get(component.part.dest);
                List<UseLink> link = new ArrayList<UseLink>();
                link.add(component.part);
                Map<Integer, List<Integer>> counterMap =  LibreBufferBuilder.computeForwardingPattern(link);
                final int triggerCounter = counterMap.keySet().iterator().next(); // get first key
                final List<Integer> destComponentIndices = counterMap.values().iterator().next(); // get first value
                final int cycle = component.part.pace / component.part.component.pace;

                buffer.addCallback(new WindowResultCallback() {

                    int suppressCount = component.suppressCount;
                    int transferCount = component.transferCount;
                    int count = 0, step = 0;

                    @Override
                    public void process(Tuple tuple) {
                        if  (count >= suppressCount && count < transferCount+suppressCount) {
                            if (step == triggerCounter) {
                                buf.put(tuple, destComponentIndices);
                                count++;
                            }
                            step = (step + 1) % cycle;
                        }
                    }
                });

                // total times run
                buffer.register(component.suppressCount+component.transferCount);
            }

        }

        return coldBuffers;
    }



    public List<ColdBuffer> build (Map<String, LibreBuffer> buffers,
                                   Collection<List<UseLink>> partitions,
                                   Functional function,
                                   Fields selectField) {
        List<ColdComponent> allColdComponents = new ArrayList<ColdComponent>();
        for (List<UseLink> partition : partitions) {
            allColdComponents.addAll(getColdComponents(partition));
        }

        return buildColdBuffers(allColdComponents, buffers, function, selectField);
    }
}
