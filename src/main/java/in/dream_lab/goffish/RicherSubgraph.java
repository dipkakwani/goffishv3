package in.dream_lab.goffish;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;

import in.dream_lab.goffish.api.IEdge;
import in.dream_lab.goffish.api.IVertex;

/*
 * Extends Subgraph and has more Features that can be used for smaller graphs
 * without running out of memory.
 */
public class RicherSubgraph<S extends Writable, V extends Writable, E extends Writable, I extends Writable, J extends Writable, K extends Writable> extends Subgraph<S, V, E, I, J, K> {
  
  private List<IEdge<E, I, J>> _edges;

  RicherSubgraph(int partitionID, K subgraphID) {
    super(partitionID, subgraphID);
    _edges = new ArrayList<IEdge<E, I, J>>();
  }
  
  @Override
  void addVertex(IVertex<V, E, I, J> v) {
    super.addVertex(v);
    if (v.outEdges() != null) {
      _edges.addAll(v.outEdges());
    }
  }

}
