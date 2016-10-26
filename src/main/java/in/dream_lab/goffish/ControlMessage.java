package in.dream_lab.goffish;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableUtils;

public class ControlMessage implements IControlMessage{

  IControlMessage.TransmissionType transmissionType;
  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeEnum(out, transmissionType);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    transmissionType = WritableUtils.readEnum(in, IControlMessage.TransmissionType.class);
  }

  @Override
  public TransmissionType getTransmissionType() {
    return transmissionType;
  }

}
