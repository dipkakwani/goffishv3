package in.dream_lab.goffish;

import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.PartitioningRunner.RecordConverter;

public interface InputReaderInterface<KIn extends Writable, VIn extends Writable, S extends Writable, V extends Writable, E extends Writable, M extends Writable> {

}
