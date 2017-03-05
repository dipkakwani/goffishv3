/*
 *      Copyright 2017 DREAM:Lab, Indian Institute of Science, Bangalore
 *
 *      Licensed under the Apache License, Version 2.0 (the "License"); you may
 *      not use this file except in compliance with the License. You may obtain
 *      a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *      Unless required by applicable law or agreed to in writing, software
 *      distributed under the License is distributed on an "AS IS" BASIS,
 *      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *      See the License for the specific language governing permissions and
 *      limitations under the License.
 */

package in.dream_lab.goffish.sample;

import java.io.*;
import java.nio.file.*;
import java.util.*;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;

import in.dream_lab.goffish.SubgraphCompute;
import in.dream_lab.goffish.api.IEdge;
import in.dream_lab.goffish.api.IMessage;
import in.dream_lab.goffish.api.IRemoteVertex;
import in.dream_lab.goffish.api.IVertex;

/*
 * Ported from Goffish V2
 * Uses Subgraph Count inverse method for blockrank initialization
 * and, Pagerank like distribution for blockrank
 */
public class BlockRankSubgraph extends
    SubgraphCompute<LongWritable, LongWritable, LongWritable, BytesWritable, LongWritable, LongWritable, LongWritable> {

  private static final double D = 0.85D;
  private static final double LOCALPAGERANK_EPSILON = .1D;
  private static final double BLOCKRANK_EPSILON = .001D;
  private static final double PAGERANK_EPSILON = .001D;

  private Map<Long, Double> _localPageRanks;
  private Map<Long, Double> _intermediateLocalSums;

  private double _blockPageRank;
  private int _numBlockOutEdges;
  private int _numSubgraphs;
  private int _numVertices;

  private boolean _inBlockRankMode;
  private boolean _firstBlockRankStep;
  private boolean _blockRankAggregateReady;
  private boolean _firstPageRankStep;
  private boolean _pageRankAggregateReady;

  private int _numBlockRankFinishes;
  private int _numPageRankFinishes;

  private void initialize() {
    _localPageRanks = new HashMap<>((int)getSubgraph().vertexCount(), 1f);
    _intermediateLocalSums = new HashMap<>((int)getSubgraph().vertexCount(), 1f);

    for (IVertex<LongWritable, LongWritable, LongWritable, LongWritable> vertex : getSubgraph().getVertices()) {
      if (vertex.isRemote()) {
        continue;
      }

      _localPageRanks.put(vertex.getVertexID().get(),
          1.0D / getSubgraph().localVertexCount());
    }

    _numBlockOutEdges = (int)(getSubgraph().vertexCount()
        - getSubgraph().localVertexCount());
    _numSubgraphs = 0;
    _numVertices = 0;

    _inBlockRankMode = true;
    _firstBlockRankStep = true;
    _blockRankAggregateReady = false;
    _firstPageRankStep = true;
    _pageRankAggregateReady = false;

    BytesWritable s1 = new BytesWritable(
        Message.subgraphMessage().toBytes());
    BytesWritable s2 = new BytesWritable(
        Message.numVerticesMessage((int) getSubgraph().localVertexCount())
            .toBytes());
      sendToAll(s1);
      sendToAll(s2);
    
  }

  @Override
  public void compute(Collection<IMessage<LongWritable, BytesWritable>> stuff) {

    _numBlockRankFinishes = 0;
    _numPageRankFinishes = 0;

    List<Message> messages = decode(stuff);

    
    if (getSuperStep() == 0) {
      initialize();

      // calculate local page ranks
      double e;
      do {
        e = doPageRankIteration(true, Collections.<Message> emptyList(), false);
      } while (e >= LOCALPAGERANK_EPSILON);

      return;
    } else if (getSuperStep() == 1) {
      _blockPageRank = 1.0D / _numSubgraphs;
    }

    if (_inBlockRankMode) {
      // calculate block rank
      if (!_firstBlockRankStep) {

        if (_blockRankAggregateReady) {
          if (_numBlockRankFinishes == _numSubgraphs) {
            _inBlockRankMode = false;
          }
        }

        if (_inBlockRankMode) {
          double sum = 0;
          for (Message message : messages) {
            sum += message.SentPageRank;
          }
          double newBlockPageRank = (1 - D) / _numSubgraphs + D * sum;
          double L1norm = Math.abs(_blockPageRank - newBlockPageRank);
          _blockPageRank = newBlockPageRank;

          if (L1norm < BLOCKRANK_EPSILON / _numSubgraphs) {
            // notify everyone that we're ok to finish
            BytesWritable s = new BytesWritable(
                Message.finishBlockRankMessage().toBytes());
            sendToAll(s);
          }

          _blockRankAggregateReady = true;
        }

        // make sure we don't accidentally use this later
        messages = Collections.emptyList();
      }

      if (_inBlockRankMode) {
        // send outgoing block rank
        Message m = new Message(_blockPageRank / _numBlockOutEdges);
        for (IRemoteVertex<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable> remoteVertex : getSubgraph().getRemoteVertices()) {
          BytesWritable s = new BytesWritable(m.toBytes());
          sendMessage(remoteVertex.getSubgraphID(), s);
        }
      }

      if (_firstBlockRankStep) {
        _firstBlockRankStep = false;
      }
    }

    if (!_inBlockRankMode) {
      // calculate approx global page rank
      if (_firstPageRankStep) {
        for (Map.Entry<Long, Double> entry : _localPageRanks.entrySet()) {
          entry.setValue(entry.getValue() * _blockPageRank);
        }

        doPageRankIteration(false, messages, true);
      } else {
        if (_pageRankAggregateReady) {
          if (_numPageRankFinishes == _numSubgraphs) {
            voteToHalt();

            // output results
            try (BufferedWriter output = new BufferedWriter(
                new OutputStreamWriter(
                    Files.newOutputStream(Paths.get("ss_" + getSuperStep()
                        + "_subgraph_" + getSubgraph().getSubgraphID().get() + "_output"))))) {
              for (IVertex<LongWritable, LongWritable, LongWritable, LongWritable> vertex : getSubgraph().getVertices()) {
                if (!vertex.isRemote()) {
                  output.write(
                      vertex.getVertexID().get() + " " + _localPageRanks.get(vertex.getVertexID().get())
                          + System.lineSeparator());
                }
              }
            } catch (IOException e) {
              System.out.println(e);
            }
          }
        }

        if (!hasVotedToHalt()) {
          double L1norm = doPageRankIteration(false, messages, false);

          if (L1norm < PAGERANK_EPSILON / _numSubgraphs) {
            // notify everyone that we're ok to finish
            BytesWritable s = new BytesWritable(
                Message.finishPageRankMessage().toBytes());
            sendToAll(s);
          }

          _pageRankAggregateReady = true;
        }
      }

      if (_firstPageRankStep) {
        _firstPageRankStep = false;
      }
    }
  }

  private double doPageRankIteration(boolean localOnly,
      Iterable<Message> messages, boolean sendMessagesOnly) {
    double L1norm = 0;
    int numVertices = (localOnly ? (int)getSubgraph().vertexCount() : _numVertices);

    for (IVertex<LongWritable, LongWritable, LongWritable, LongWritable> vertex : getSubgraph().getVertices()) {
      if (vertex.isRemote()) {
        continue;
      }

      _intermediateLocalSums.put(vertex.getVertexID().get(), 0.0D);
    }

    // calculate sums from this subgraphs
    for (IVertex<LongWritable, LongWritable, LongWritable, LongWritable> vertex : getSubgraph().getVertices()) {
      if (vertex.isRemote()) {
        continue;
      }

      int outDegree = 0;
      for (IEdge<LongWritable, LongWritable, LongWritable> edge : vertex
          .outEdges()) {
        if (!localOnly || !getSubgraph().getVertexByID(edge.getSinkVertexID())
            .isRemote()) {
          outDegree++;
        }
      }

      double sentWeight = _localPageRanks.get(vertex.getVertexID().get()) / outDegree;

      for (IEdge<LongWritable, LongWritable, LongWritable> edge : vertex.outEdges()) {
        LongWritable sinkVertexId = edge.getSinkVertexID();
        if (getSubgraph().getVertexByID(sinkVertexId).isRemote()) {
          if (!localOnly) {
            Message m = new Message(sinkVertexId.get(), sentWeight);
            BytesWritable s = new BytesWritable(m.toBytes());
            IRemoteVertex<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable> remoteVertex = (IRemoteVertex<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable>) getSubgraph()
                .getVertexByID(sinkVertexId);
            sendMessage(remoteVertex.getSubgraphID(), s);
          }
        } else {
          _intermediateLocalSums.put(sinkVertexId.get(),
              _intermediateLocalSums.get(sinkVertexId) + sentWeight);
        }
      }
    }

    if (!localOnly) {
      for (Message message : messages) {
        _intermediateLocalSums.put(message.LocalVertexId,
            _intermediateLocalSums.get(message.LocalVertexId)
                + message.SentPageRank);
      }
    }

    // update weights
    if (!sendMessagesOnly) {
      for (IVertex<LongWritable, LongWritable, LongWritable, LongWritable> vertex : getSubgraph().getVertices()) {
        if (vertex.isRemote()) {
          continue;
        }

        double oldPageRank = _localPageRanks.get(vertex.getVertexID().get());
        double newPageRank = (1 - D) / numVertices
            + D * _intermediateLocalSums.get(vertex.getVertexID().get());
        _localPageRanks.put(vertex.getVertexID().get(), newPageRank);
        L1norm += Math.abs(newPageRank - oldPageRank);
      }
    }

    return L1norm;
  }

  private List<Message> decode(Collection<IMessage<LongWritable, BytesWritable>> stuff) {
    if (stuff.isEmpty()) {
      return Collections.emptyList();
    }

    ArrayList<Message> messages = new ArrayList<>(stuff.size());
    for (IMessage<LongWritable, BytesWritable> s : stuff) {
      Message m = Message.fromBytes(s.getMessage().getBytes());
      if (m.SubgraphMessage) {
        _numSubgraphs++;
      } else if (m.FinishBlockRank) {
        _numBlockRankFinishes++;
      } else if (m.FinishPageRank) {
        _numPageRankFinishes++;
      } else if (m.NumVertices > 0) {
        _numVertices += m.NumVertices;
      } else {
        messages.add(m);
      }
    }

    return messages;
  }

  static class Message {

    final boolean SubgraphMessage;
    final boolean FinishBlockRank;
    final boolean FinishPageRank;
    final int NumVertices;
    final long LocalVertexId;
    final double SentPageRank;

    private Message(boolean subgraphMessage, boolean finishBlockRank,
        boolean finishPageRank) {
      SubgraphMessage = subgraphMessage;
      FinishBlockRank = finishBlockRank;
      FinishPageRank = finishPageRank;
      LocalVertexId = Long.MIN_VALUE;
      SentPageRank = 0;
      NumVertices = 0;
    }

    static Message subgraphMessage() {
      return new Message(true, false, false);
    }

    static Message finishBlockRankMessage() {
      return new Message(false, true, false);
    }

    static Message finishPageRankMessage() {
      return new Message(false, false, true);
    }

    private Message(int numVertices) {
      LocalVertexId = Long.MIN_VALUE;
      FinishBlockRank = false;
      FinishPageRank = false;
      SentPageRank = 0;
      NumVertices = numVertices;
      SubgraphMessage = false;
    }

    static Message numVerticesMessage(int numVertices) {
      return new Message(numVertices);
    }

    Message(double sentPageRank) {
      LocalVertexId = Long.MIN_VALUE;
      FinishBlockRank = false;
      FinishPageRank = false;
      SentPageRank = sentPageRank;
      NumVertices = 0;
      SubgraphMessage = false;
    }

    Message(long localVertexId, double sentPageRank) {
      LocalVertexId = localVertexId;
      FinishBlockRank = false;
      FinishPageRank = false;
      SentPageRank = sentPageRank;
      NumVertices = 0;
      SubgraphMessage = false;
    }

    private Message(boolean subgraphMessage, boolean finishBlockRank,
        boolean finishPageRank, int numVertices, long localVertexId,
        double sentPageRank) {
      SubgraphMessage = subgraphMessage;
      FinishBlockRank = finishBlockRank;
      FinishPageRank = finishPageRank;
      NumVertices = numVertices;
      LocalVertexId = localVertexId;
      SentPageRank = sentPageRank;
    }

    byte[] toBytes() {
      return (Boolean.toString(SubgraphMessage) + ","
          + Boolean.toString(FinishBlockRank) + ","
          + Boolean.toString(FinishPageRank) + ","
          + Integer.toString(NumVertices) + "," + Long.toString(LocalVertexId)
          + "," + Double.toString(SentPageRank)).getBytes();
    }

    static Message fromBytes(byte[] bytes) {
      String[] s = new String(bytes).split(",");
      return new Message(Boolean.parseBoolean(s[0]), Boolean.parseBoolean(s[1]),
          Boolean.parseBoolean(s[2]), Integer.parseInt(s[3]),
          Long.parseLong(s[4]), Double.parseDouble(s[5]));
    }
  }
}
