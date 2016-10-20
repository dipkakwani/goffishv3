package in.dream_lab.goffish;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.sync.SyncException;

public interface IReader<KIn extends Writable, VIn extends Writable, KOut extends Writable, VOut extends Writable, S extends Writable, V extends Writable, E extends Writable, I extends Writable, J extends Writable, K extends Writable> {
  List<ISubgraph<S, V, E, I, J, K>> getSubgraphs()throws IOException, SyncException, InterruptedException ;
}
