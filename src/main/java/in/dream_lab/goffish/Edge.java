package in.dream_lab.goffish;

public class Edge {
  Vertex _source;
  Vertex _sink;
  
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
