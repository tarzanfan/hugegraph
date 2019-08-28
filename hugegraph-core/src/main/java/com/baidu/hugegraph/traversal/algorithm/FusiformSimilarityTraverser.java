/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.traversal.algorithm;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.CollectionUtil;
import com.baidu.hugegraph.util.E;

public class FusiformSimilarityTraverser extends HugeTraverser {

    public FusiformSimilarityTraverser(HugeGraph graph) {
        super(graph);
    }

    public Map<Id, Set<Id>> fusiformSimilarity(List<HugeVertex> vertices,
                                               Directions direction, Id label,
                                               int minEdgeCount, long degree,
                                               float alpha, int top,
                                               String groupProperty,
                                               int minGroupCount,
                                               long capacity, long limit) {
        checkCapacity(capacity);
        checkLimit(limit);
        int loop = 0;
        long accessed = vertices.size();
        Map<Id, Set<Id>> results = new LinkedHashMap<>(vertices.size());
        for (HugeVertex vertex : vertices) {
            Iterator<Edge> edges = this.edgesOfVertex(vertex.id(), direction,
                                                      label, minEdgeCount);
            long edgeCount = IteratorUtils.count(edges);
            if (edgeCount < minEdgeCount) {
                // Ignore current vertex if its matched edges number not enough
                continue;
            }

            // Get similar nodes and counts
            edges = this.edgesOfVertex(vertex.id(), direction, label, degree);
            Map<Id, Integer> similars = new HashMap<>();
            int neighborCount = 0;
            while (edges.hasNext()) {
                neighborCount++;
                checkCapacity(capacity, ++accessed, "fusiform similarity");
                Id target = ((HugeEdge) edges.next()).id().otherVertexId();
                Directions backDir = direction.opposite();
                Iterator<Edge> backEdges = this.edgesOfVertex(target, backDir,
                                                              label, degree);
                while (backEdges.hasNext()) {
                    HugeEdge backEdge = (HugeEdge) backEdges.next();
                    Id node = backEdge.id().otherVertexId();
                    if (similars.containsKey(node)) {
                        similars.put(node, similars.get(node) + 1);
                    } else {
                        similars.put(node, 1);
                        checkCapacity(capacity, ++accessed,
                                      "fusiform similarity");
                    }
                }
            }

            // Delete source vertex
            assert similars.containsKey(vertex.id());
            similars.remove(vertex.id());
            if (similars.isEmpty()) {
                continue;
            }

            // Match alpha
            int alphaCount = (int) (neighborCount * alpha);
            Map<Id, Integer> matchedAlpha = new HashMap<>();
            for (Map.Entry<Id, Integer> entry : similars.entrySet()) {
                if (entry.getValue() >= alphaCount) {
                    matchedAlpha.put(entry.getKey(), entry.getValue());
                }
            }
            if (matchedAlpha.isEmpty()) {
                continue;
            }

            // Sorted and topN if needed
            if (top > 0) {
                similars = CollectionUtil.sortByValue(matchedAlpha, false);
                similars = topN(similars, true, top);
            } else {
                similars = matchedAlpha;
            }

            // Filter by groupCount by property
            if (groupProperty != null) {
                E.checkArgument(minGroupCount > 0,
                                "Must set min group count when group " +
                                "property set");
                Set<Object> values = new HashSet<>();
                // Add groupProperty value of source vertex
                values.add(vertex.value(groupProperty));
                for (Id id : similars.keySet()) {
                    Vertex v = graph().vertices(id).next();
                    values.add(v.value(groupProperty));
                }
                if (values.size() < minGroupCount) {
                    continue;
                }
            }
            results.put(vertex.id(), similars.keySet());
            // Reach limit
            if (limit != NO_LIMIT && ++loop >= limit) {
                break;
            }
        }
        return results;
    }
}
