/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package in.dream_lab.goffish.sample;

import in.dream_lab.goffish.SubgraphCompute;
import in.dream_lab.goffish.api.IEdge;
import in.dream_lab.goffish.api.IMessage;
import in.dream_lab.goffish.api.IRemoteVertex;
import in.dream_lab.goffish.api.IVertex;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.json.simple.JSONArray;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Created by Diptanshu Kakwani on 28/3/17.
 * Assigns discrete random weights in the range (1-5) to each edge in the graph,
 * based on uniform distribution.
 */
public class GenerateWeights extends
    SubgraphCompute<LongWritable, LongWritable, LongWritable, Text, LongWritable, LongWritable, LongWritable> {

  public static final Log LOG = LogFactory
      .getLog(GenerateWeights.class);

  Map<Pair<Long, Long>, Integer> edgeWeight = new HashMap<>();
  Map<LongWritable, StringBuilder> messages = new HashMap<>();

  // To represent end points of edge.
  private class Pair<L, R> {
    L first;
    R second;

    Pair(L a, R b) {
      first = a;
      second = b;
    }

    @Override
    public int hashCode() {
      return (first == null ? 0 : first.hashCode()) ^ (second == null ? 0 : second.hashCode());
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Pair)) {
        return false;
      }
      Pair<?, ?> p = (Pair<?, ?>) o;
      return first.equals(p.first) && second.equals(p.second);
    }
  }

  @Override
  public void compute(Iterable<IMessage<LongWritable, Text>> iMessages) {

    if (getSuperstep() == 0) {
      LOG.info("Starting first superstep");
      Random ran = new Random();
      int[] numsToGenerate = new int[]{1, 2, 3, 4, 5};

      /* Number of weights to be assigned. We don't want to assign different weights to the same edge. */
      int numWeights = 0;


      for (IVertex<LongWritable, LongWritable, LongWritable, LongWritable> vertex : getSubgraph()
          .getLocalVertices()) {
        for (IEdge<LongWritable, LongWritable, LongWritable> e : vertex
            .getOutEdges()) {
          if (vertex.getVertexId().get() < e.getSinkVertexId().get()) {
            int randomWeight = numsToGenerate[ran.nextInt(5)];
            edgeWeight.put(new Pair<Long, Long>(vertex.getVertexId().get(), e.getSinkVertexId().get()), randomWeight);
            edgeWeight.put(new Pair<Long, Long>(e.getSinkVertexId().get(), vertex.getVertexId().get()), randomWeight);

            // If the edge connects to a remote vertex, pack the edge weight into messages.
            IVertex sink = getSubgraph().getVertexById(e.getSinkVertexId());
            if (sink.isRemote()) {
              StringBuilder msg = messages.get(((IRemoteVertex<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable>)
                  sink).getSubgraphId());
              if (msg == null) {
                msg = new StringBuilder();
                messages.put(((IRemoteVertex<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable>)
                    sink).getSubgraphId(), msg);
              }
              msg.append(e.getSinkVertexId()).append(',').append(vertex.getVertexId().toString()).append(',').append(Integer.toString(randomWeight))
                  .append(';');
            }
          }
        }
      }
      LOG.info("Weights generated!");
      sendPackedMessages();
    } else {
      LOG.info("Second Superstep");
      for (IMessage<LongWritable, Text> m : iMessages) {
        String msg = m.getMessage().toString();
        String[] vertexIds = msg.split(";");
        for (int i = 0; i < vertexIds.length; i++) {
          String[] edgePair = vertexIds[i].split(",");
          Long sourceId = Long.parseLong(edgePair[0]);
          Long sink = Long.parseLong(edgePair[1]);
          assert (edgeWeight.get(new Pair<Long, Long>(sourceId, sink)) == null);
          edgeWeight.put(new Pair<Long, Long>(sourceId, sink), Integer.parseInt(edgePair[2]));
          edgeWeight.put(new Pair<Long, Long>(sink, sourceId), Integer.parseInt(edgePair[2]));
        }
      }
      LOG.info("Writing JSONArray to file");
      try {
        FileWriter file = new FileWriter("/home/humus/weightedOrkut/giraph/subgraph" + getSubgraph().getSubgraphId().get());
        BufferedWriter out = new BufferedWriter(file);
        for (IVertex<LongWritable, LongWritable, LongWritable, LongWritable> vertex : getSubgraph()
            .getLocalVertices()) {
          JSONArray jsonOutput = new JSONArray();
          jsonOutput.add(vertex.getVertexId().get());
          jsonOutput.add(new Integer(1));
          JSONArray edgeArray = new JSONArray();
          for (IEdge<LongWritable, LongWritable, LongWritable> e : vertex
              .getOutEdges()) {
            JSONArray edge = new JSONArray();
            edge.add(e.getSinkVertexId().get());
            //edge.add(e.getEdgeId());  // Not required for giraph
            edge.add(edgeWeight.get(new Pair<Long, Long>(vertex.getVertexId().get(), e.getSinkVertexId().get())));
            edgeArray.add(edge);
          }
          jsonOutput.add(edgeArray);
          out.write(jsonOutput.toString());
          out.newLine();
        }
        out.close();
        file.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    voteToHalt();

  }

  void sendPackedMessages() {
    LOG.info("Sending messages, count: " + messages.size());
    for (Map.Entry<LongWritable, StringBuilder> entry : messages.entrySet()) {
      sendMessage(entry.getKey(), new Text(entry.getValue().toString()));
    }
  }
}
