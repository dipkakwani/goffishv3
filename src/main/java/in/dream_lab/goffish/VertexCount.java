package in.dream_lab.goffish;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.bsp.BSPPeer;

public class VertexCount extends Subgraph {
  
  void compute(List<Text> messages) {
    if (getSuperStep() == 0) {
      long count = 0;
      for (Vertex v : getVertices()) {
        if (!v.isRemote()) {
          count++;
        }
      }
      try{
        Text message=new Text(new Long(count).toString());
        for (String peers:peer.getAllPeerNames()){
          peer.send(peers, message);
        }
      }
      catch(IOException e)
      {}
    }
    else {
      long totalVertices=0;
      for(Text msg:messages){
        String msgString = msg.toString();
        totalVertices+=Long.parseLong(msgString);
      }
      try {
          peer.write(new LongWritable(getSubgraphID()), new LongWritable(totalVertices));
	} catch (IOException e) {
	  // TODO Auto-generated catch block
	  e.printStackTrace();
	}
    }
  }
  
  /*FIXME: Shouldn't be in user's logic. */
  VertexCount(long subgraphID, 
      BSPPeer<LongWritable, LongWritable, LongWritable, LongWritable, Text> peer) {
    super(subgraphID,  peer);
  }
}
