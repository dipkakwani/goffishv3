package in.dream_lab.goffish;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.Partitioner;
import org.apache.hama.commons.util.KeyValuePair;
import org.apache.hama.util.ReflectionUtils;

/* FIXME: Message using GraphJobMessage. It should allow to send plain string, required during setup. */
public class EdgeListReader<KOut extends Writable, VOut extends Writable, S extends Writable, V extends Writable, E extends Writable, M extends Writable> 
 implements InputReaderInterface <LongWritable, Text, KOut, VOut, S, V, E, M>{
  
  Map<Long, Vertex<V, E>> vertexMap;
  BSPPeer<LongWritable, Text, KOut, VOut, GraphJobMessage<S, V, E, M>> peer;
  Partition<S, V, E, M> partition;
  
  List<Subgraph<S, V, E, M>> getSubgraphs() {
    Map<Long, Vertex<V, E>> vertexMap = new HashMap<Long, Vertex<V, E>>();
    List<Vertex<V, E>> verticesList = new ArrayList<Vertex<V, E>>();
    
    KeyValuePair<LongWritable, Text> pair;
    long numPeers = peer.getNumPeers();
    while ((pair = peer.readNext()) != null) {
      //long sourceID = pair.getKey().get();
      String value[] = pair.getValue().toString().split("\t");
      long sourceID = Long.parseLong(value[0]);
      String edgeList[] = value[1].split(" ");
      int targetSourcePeer = (int) (sourceID % numPeers);
      for (String dest : edgeList) {
        long sinkID = Long.parseLong(dest);
        int targetSinkPeer = (int) (sinkID % numPeers);
        Vertex<V, E> source = vertexMap.get(sourceID);
        Vertex<V, E> sink = vertexMap.get(sinkID);
        if (source == null) {
          source = new Vertex<V, E>(sourceID, targetSourcePeer);
          vertexMap.put(sourceID, source);
          verticesList.add(source);
        }
        if (sink == null) {
          sink = new Vertex<V, E>(sinkID, targetSinkPeer);
          vertexMap.put(sinkID, sink);
          verticesList.add(sink);
        }
        Edge<V, E> e = new Edge<V, E>(source, sink);
        source.addEdge(e);
      }
    }
    List<Vertex<V, E>> _vertices = new ArrayList<Vertex<V, E>>(); // Final list of vertices.
    vertexMap = new HashMap<Long, Vertex<V, E>>();

    // Send vertices to their respective partitions.
    for (Vertex<V, E> v : verticesList) {
      int targetPeer = getPartitionID(v);
      if (targetPeer != peer.getPeerIndex()) {
        GraphJobMessage<S, V, E, M> msg = new GraphJobMessage<S, V, E, M>(v);
        //Text msg = new Text(sb.toString());
        peer.send(peer.getPeerName(targetPeer), msg);
      } else { // Belongs to this partition
        _vertices.add(v);
        vertexMap.put(v.getVertexID(), v);
      }
    }
    for (Vertex<V, E> v : _vertices) {
      System.out.println(v.getVertexID());
    }
    
    System.out.println(_vertices.size()+"=size="+vertexMap.size());

    // End of first superstep.
    peer.sync();
    
    Text msg;
    while ((msg = peer.getCurrentMessage()) != null) {
      String msgString = msg.toString();
      String msgStringArr[] = msgString.split(",");
      long vertexID = Long.parseLong(msgStringArr[0]);
      Vertex<V, E> v = new Vertex<V, E>(vertexID, getPartitionID(vertexID));
      _vertices.add(v);
      vertexMap.put(v.getVertexID(), v);
      for (int i = 1; i < msgStringArr.length; i++) {
        long sinkID = Long.valueOf(msgStringArr[i]);
        Vertex<V, E> sink = vertexMap.get(sinkID);
        if (sink == null) {
          sink = new Vertex<V, E>(sinkID, (int)(sinkID % numPeers));
          vertexMap.put(sinkID, sink);
        }
        Edge<V, E> e = new Edge<V, E>(v, sink);
        v.addEdge(e);
      }
    }
    formSubgraphs(_vertices);

    /*
     * Ask Remote Vertices to send their subgraph IDs. Requires 2 supersteps
     * because the graph is directed
     */
    for (Vertex<V, E> v : _vertices) {
      if (v.isRemote()) {
        msg = new Text(v.getVertexID() + "," + peer.getPeerIndex());
        peer.send(peer.getPeerName(getPartitionID(v)), msg);
      }
    }

    peer.sync();

    while ((msg = peer.getCurrentMessage()) != null) {
      String msgString = msg.toString();
      String msgStringArr[] = msgString.split(",");
      Long sinkID = Long.valueOf(msgStringArr[0]);
      for (Vertex<V, E> v : _vertices) {
        if (sinkID == v.getVertexID()) {
          peer.send(peer.getPeerName(Integer.parseInt(msgStringArr[1])),
              new Text(v.getVertexID() + "," + v.getSubgraphID()));
        }
      }
    }
    
    peer.sync();
    System.out.println("Messages to all neighbours sent");
    
    while ((msg = peer.getCurrentMessage()) != null) {
      String msgString = msg.toString();
      String msgStringArr[] = msgString.split(",");
      Long sinkID = Long.parseLong(msgStringArr[0]);
      Long remoteSubgraphID = Long.parseLong(msgStringArr[1]);
      for(Vertex<V, E> v : _vertices) {
        if (v.getVertexID() == sinkID) {
          v.setRemoteSubgraphID(remoteSubgraphID);
        }
      }
    }
  }
  
  /* Forms subgraphs by finding (weakly) connected components. */
  void formSubgraphs(List<Vertex<V, E>> vertices) {
    long subgraphCount = 0;
    Set<Long> visited = new HashSet<Long>();

    for (Vertex<V, E> v : vertices) {
      if (!visited.contains(v.getVertexID())) {
        long subgraphID = subgraphCount++ | (((long) partition.getPartitionID()) << 32);
        Subgraph subgraph = new VertexCount.VrtxCnt(subgraphID, peer);
        dfs(v, visited, subgraph);
        partition.addSubgraph(subgraph);
        System.out.println("Subgraph " + subgraph.getSubgraphID() + "has "
            + subgraph.vertexCount() + "Vertices");
      }
    }
  }
  
  void dfs(Vertex<V, E> v, Set<Long> visited, Subgraph subgraph) {
    if (peer.getPeerIndex() == getPartitionID(v)) {
      v.setSubgraphID(subgraph.getSubgraphID());
      subgraph.addLocalVertex(v);
    } else {
      v.setSubgraphID(-1);
      subgraph.addRemoteVertex(v);
    }
    subgraph.addVertex(v);
    visited.add(v.getVertexID());
    for (Edge<V, E> e : v.outEdges()) {
      subgraph.addEdge(e);
      Vertex<V, E> sink = e.getSink();
      if (!visited.contains(sink.getVertexID())) {
        dfs(sink, visited, subgraph);
      }
    }
  }

  
  
  int getPartitionID(Vertex<V, E> v){
    return (int) v.getVertexID() % peer.getNumPeers();
  }
  
  int getPartitionID(long vertexID){
    return (int) vertexID % peer.getNumPeers();
  }
  
  public EdgeListReader(BSPPeer<LongWritable, Text, KOut, VOut, GraphJobMessage<S, V, E, M>> peer, Partition<S, V, E, M> partition) {
    this.peer = peer;
    this.partition = partition;
  }
  
  /*TODO: Move this to GraphJobRunner. */
  public static <S extends Writable, V extends Writable, E extends Writable, M extends Writable> Subgraph<S, V, E, M> newSubgraphInstance(Class<?> subgraphClass) {
    return (Subgraph<S, V, E, M>) ReflectionUtils.newInstance(subgraphClass);
  }

}
