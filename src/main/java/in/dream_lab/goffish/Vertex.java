package in.dream_lab.goffish;

import java.util.ArrayList;
import java.util.List;

public class Vertex {
  private List<Edge> _adjList;
  private long vertexID;
  private long subgraphID;
  private long remoteSubgraphID;
  private int partitionID;
  
  Vertex(long ID, int partitionID) {
    vertexID = ID;
    this.partitionID = partitionID;
    _adjList = new ArrayList<Edge>();
    remoteSubgraphID = -1;
  }
  
  void addEdge(Vertex destination) {
    Edge e = new Edge(this, destination);
    _adjList.add(e);
  }
  
  void addEdge(Edge e) {
    _adjList.add(e);
  }
  
  long getVertexID() {
    return vertexID;
  }
  
  void setSubgraphID(long ID) {
    subgraphID = ID;
  }
  
  void setRemoteSubgraphID(long ID) {
    remoteSubgraphID = ID;
  }
  
  boolean isRemote() {
    return (remoteSubgraphID != -1);
  }
  
  List<Edge> outEdges() {
    return _adjList;
  }
  
  int getPartitionID() {
    return partitionID;
  }
  
  long getSubgraphID() {
    return subgraphID;
  }
}
