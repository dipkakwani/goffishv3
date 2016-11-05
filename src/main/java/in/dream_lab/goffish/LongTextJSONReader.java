package in.dream_lab.goffish;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.commons.util.KeyValuePair;
import org.json.simple.JSONArray;
import org.json.simple.JSONValue;

/*
 * Reads the graph from JSON format
 * [srcid,pid, srcvalue, [[sinkid1,edgeid1,edgevalue1], [sinkid2,edgeid2,edgevalue2] ... ]] 
 */

public class LongTextJSONReader<S extends Writable, V extends Writable, E extends Writable, K extends Writable, M extends Writable>
    implements
    IReader<Writable, Writable, Writable, Writable, S, V, E, LongWritable, LongWritable, LongWritable> {

  Map<LongWritable, IVertex<V, E, LongWritable, LongWritable>> vertexMap;
  BSPPeer<Writable, Writable, Writable, Writable, Message<K, M>> peer;
  private Map<K, Integer> subgraphPartitionMap;

  public LongTextJSONReader(
      BSPPeer<Writable, Writable, Writable, Writable, Message<K, M>> peer,
      Map<K, Integer> subgraphPartitionMap) {
    this.peer = peer;
    this.subgraphPartitionMap = subgraphPartitionMap;
  }

  @Override
  public List<ISubgraph<S, V, E, LongWritable, LongWritable, LongWritable>> getSubgraphs()
      throws IOException, SyncException, InterruptedException {

    // Map of partitionID,vertex that do not belong to this partition
    Map<Integer, List<String>> partitionMap = new HashMap<Integer, List<String>>();

    vertexMap = new HashMap<LongWritable, IVertex<V, E, LongWritable, LongWritable>>();

    // List of edges.Used to create RemoteVertices
    List<IEdge<E, LongWritable, LongWritable>> _edges = new ArrayList<IEdge<E, LongWritable, LongWritable>>();

    KeyValuePair<Writable, Writable> pair;
    while ((pair = peer.readNext()) != null) {
      String StringJSONInput = pair.getValue().toString();
      JSONArray JSONInput = (JSONArray) JSONValue.parse(StringJSONInput);

      int partitionID = Integer.parseInt(JSONInput.get(1).toString());

      // Vertex does not belong to this partition
      if (partitionID != peer.getPeerIndex()) {
        List<String> partitionVertices = partitionMap.get(partitionID);
        if (partitionVertices == null) {
          partitionVertices = new ArrayList<String>();
          partitionMap.put(partitionID, partitionVertices);
        }
        partitionVertices.add(StringJSONInput);
      } else {
        Vertex<V, E, LongWritable, LongWritable> vertex = createVertex(
            StringJSONInput);
        vertexMap.put(vertex.getVertexID(), vertex);
        _edges.addAll(vertex.outEdges());
      }
    }

    // Send vertices to their respective partitions
    for (Map.Entry<Integer, List<String>> entry : partitionMap.entrySet()) {
      int partitionID = entry.getKey().intValue();
      List<String> vertices = entry.getValue();
      for (String vertex : vertices) {
        Message<LongWritable, LongWritable> vertexMsg = new Message<LongWritable, LongWritable>();
        ControlMessage controlInfo = new ControlMessage();
        controlInfo
            .setTransmissionType(IControlMessage.TransmissionType.VERTEX);
        controlInfo.setVertexValues(vertex);
        vertexMsg.setControlInfo(controlInfo);
        peer.send(peer.getPeerName(partitionID), (Message<K, M>) vertexMsg);
      }
    }
    
    //End of first SuperStep
    peer.sync();
    Message<LongWritable, LongWritable> msg;
    while ((msg = (Message<LongWritable, LongWritable>)peer.getCurrentMessage()) != null) {
      String JSONVertex = msg.getControlInfo().toString();
      Vertex<V, E, LongWritable, LongWritable> vertex = createVertex(JSONVertex);
      vertexMap.put(vertex.getVertexID(), vertex);
      _edges.addAll(vertex.outEdges());
    }
    
    /* Create remote vertex objects. */
    for (IEdge<E, LongWritable, LongWritable> e : _edges) {
      LongWritable sinkID = e.getSinkVertexID();
      IVertex<V, E, LongWritable, LongWritable> sink =  vertexMap.get(sinkID);
      if (sink == null) {
        sink = new RemoteVertex<V, E, LongWritable, LongWritable, LongWritable>(sinkID);
        vertexMap.put(sinkID, sink);
      }
    }
    //TODO
    return null;
  }
  
  @SuppressWarnings("unchecked")
  Vertex<V, E, LongWritable, LongWritable> createVertex(String JSONString) {
    JSONArray JSONInput = (JSONArray) JSONValue.parse(JSONString);

    LongWritable sourceID = new LongWritable(
        Long.valueOf(JSONInput.get(0).toString()));
    assert (vertexMap.get(sourceID) == null);

    Vertex<V, E, LongWritable, LongWritable> vertex = new Vertex<V, E, LongWritable, LongWritable>(
        sourceID);
    V value = (V) JSONInput.get(2);
    vertex.setValue(value);

    JSONArray edgeList = (JSONArray) JSONInput.get(3);
    for (Object edgeInfo : edgeList) {
      Object edgeValues[] = ((JSONArray) edgeInfo).toArray();
      LongWritable sinkID = new LongWritable(
          Long.valueOf(edgeValues[0].toString()));
      LongWritable edgeID = new LongWritable(
          Long.valueOf(edgeValues[1].toString()));
      E edgeValue = (E) edgeValues[2];
      Edge<E, LongWritable, LongWritable> edge = new Edge<E, LongWritable, LongWritable>(
          edgeID, sinkID);
      edge.setValue(edgeValue);
      vertex.addEdge(edge);
    }
    return vertex;
  }

}
