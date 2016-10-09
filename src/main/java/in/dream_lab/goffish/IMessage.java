package in.dream_lab.goffish;

public interface IMessage <K extends Writable> {
  Enum getMessageType();        // Enum: vertex, custom-message, messagelist.
  
  K getSubgraphID();
  
  Writable getMessage();
}
