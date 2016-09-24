package in.dream_lab.goffish;

import java.util.ArrayList;
import java.util.List;

public class Partition {
  private int partitionID;
  private List<Subgraph> _subgraphs;
  
  Partition(int ID) {
    partitionID = ID;
    _subgraphs = new ArrayList<Subgraph>();
  }
  
  int getPartitionID() {
    return partitionID;
  }
  
  void addSubgraph(Subgraph subgraph) {
	  _subgraphs.add(subgraph);
  }
  
  List<Subgraph> getSubgraphs() {
	  return _subgraphs;
  }
}
