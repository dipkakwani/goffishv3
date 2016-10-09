package in.dream_lab.goffish;

import java.util.Collection;

import org.apache.hadoop.io.Writable;

/* Defines Vertex interface. Could be used to define multiple graph representation,
 * e.g: adjacency list, adjacency set.
 * @param <V> Vertex value object type
 * @param <E> Edge value object type
 * */
public interface VertexInterface<V extends Writable, E extends Writable> {
  long getVertexID();
  
  boolean isRemote();
  
  Collection<Edge<V, E>> outEdges();
  
  int getPartitionID();
  
  long getSubgraphID();
}
