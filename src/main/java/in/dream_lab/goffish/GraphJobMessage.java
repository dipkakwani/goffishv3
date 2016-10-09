package in.dream_lab.goffish;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/* User can extend it to define their own message type. By default it uses Text. */
public class GraphJobMessage <S extends Writable, V extends Writable, E extends Writable, M extends Writable> implements Writable {
  public static final int VERTEX_FLAG = 0x01;   // Sending vertex as message
  public static final int CUSTOM_MESSAGE_FLAG = 0x02;   // Custom message type. Text by default.
  private int flag = -1;
  
  GraphJobMessage(Vertex<V, E> vertex) {
    flag = VERTEX_FLAG;
    StringBuilder sb = new StringBuilder();
    sb.append(Long.toString(vertex.getVertexID()));
    for (Edge<V, E> e : vertex.outEdges()) {
      Vertex<V, E> sink = e.getSink();
      sb.append(",").append(Long.toString(sink.getVertexID()));
    }
  }
  
  GraphJobMessage(Text msg) {
    flag = CUSTOM_MESSAGE_FLAG;
  }
  
  GraphJobMessage(M msg) {
    flag = CUSTOM_MESSAGE_FLAG;
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    
}
}
