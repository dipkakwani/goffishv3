package in.dream_lab.goffish;

import java.util.ArrayList;
import java.util.List;

public class Partition <T extends Subgraph> {
  private int partitionID;
  private List<T> _subgraphs;
  
  Partition(int ID) {
    partitionID = ID;
    _subgraphs = new ArrayList<T>();
  }
  
  int getPartitionID() {
    return partitionID;
  }
}
