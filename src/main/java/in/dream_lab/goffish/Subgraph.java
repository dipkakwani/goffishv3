package in.dream_lab.goffish;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public abstract class Subgraph {
  long subgraphID;
  private List<Vertex> _vertices;
  private Map<Long, Vertex> _verticesID;
  private List<Vertex> _localVertices;
  private List<Edge> _edges;
  int partitionID;
  
  Subgraph(long subgraphID, int partitionID) {
    this.subgraphID = subgraphID;
    this.partitionID = partitionID;
    _vertices = new ArrayList<Vertex>();
    _localVertices = new ArrayList<Vertex>();
    _verticesID = new HashMap<Long, Vertex>();
  }
  
  void addVertex(Vertex v) {
    _vertices.add(v);
    _verticesID.put(v.getVertexID(), v);
  }
  
  Vertex getVertexByID(long vertexID) {
    return _verticesID.get(vertexID);
  }
  
  void addEdge(Edge e) {
    _edges.add(e);
  }
  
  long getSubgraphID() {
    return subgraphID;
  }
  
  abstract void compute(List<Text> messages);
}
