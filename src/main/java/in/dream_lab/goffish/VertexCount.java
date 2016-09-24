package in.dream_lab.goffish;

import java.util.List;

import org.apache.hadoop.io.Text;

public class VertexCount extends Subgraph {
  
  void compute(List<Text> messages) {
    
  }
  
  /*FIXME: Shouldn't be in user's logic. */
  VertexCount(long subgraphID, int partitionID) {
    super(subgraphID, partitionID);
  }
}
