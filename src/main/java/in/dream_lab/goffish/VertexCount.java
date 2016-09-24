package in.dream_lab.goffish;

import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.bsp.BSPPeer;

public class VertexCount extends Subgraph {
  
  void compute(List<Text> messages) {
    if (getSuperStep() == 0) {
      long count = 0;
      for (Vertex v : getVertices()) {
        if (v.isRemote()) {
          
        }
      }
      for (peer.getAllPeerNames())
    }
    else {
      
    }
  }
  
  /*FIXME: Shouldn't be in user's logic. */
  VertexCount(long subgraphID, 
      BSPPeer<LongWritable, LongWritable, LongWritable, LongWritable, Text> peer) {
    super(subgraphID, partitionID, peer);
  }
}
