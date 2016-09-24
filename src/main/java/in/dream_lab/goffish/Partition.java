package in.dream_lab.goffish;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Partition {
  private int partitionID;
  private List<Subgraph> _subgraphs;
  private Map<Long, Subgraph> _subgraphMap;
  
  Partition(int ID) {
    partitionID = ID;
    _subgraphs = new ArrayList<Subgraph>();
    _subgraphMap = new HashMap<Long, Subgraph>();
  }
  
  int getPartitionID() {
    return partitionID;
  }
  
  void addSubgraph(Subgraph subgraph) {
    _subgraphs.add(subgraph);
    _subgraphMap.put(subgraph.getSubgraphID(), subgraph);
  }
  
  List<Subgraph> getSubgraphs() {
    return _subgraphs;
  }
  
  Subgraph getSubgraph(long subgraphID) {
    return _subgraphMap.get(subgraphID);
  }
}
