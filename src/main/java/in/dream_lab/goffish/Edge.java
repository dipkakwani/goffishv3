package in.dream_lab.goffish;

public class Edge {
  private Vertex _source;
  private Vertex _sink;
  
  Edge(Vertex u, Vertex v) {
    _source = u;
    _sink = v;
  }
  
  Vertex getSource() {
    return _source;
  }
  
  Vertex getSink() {
    return _sink;
  }
}
