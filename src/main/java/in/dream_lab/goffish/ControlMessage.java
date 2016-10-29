/**


 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package in.dream_lab.goffish;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;

public class ControlMessage implements IControlMessage{

  private IControlMessage.TransmissionType transmissionType;
  private Text vertexValues = new Text("");
  private Text generalInfo = new Text("");
  private int partitionID;
  
  public ControlMessage() {
    transmissionType = IControlMessage.TransmissionType.NORMAL;
    generalInfo = new Text("");
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeEnum(out, transmissionType);
    generalInfo.write(out);
    if (isPartitionMessage()) {
      out.writeInt(partitionID);
    }
    else if(isVertexMessage()) {
      vertexValues.write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    transmissionType = WritableUtils.readEnum(in, IControlMessage.TransmissionType.class);
    generalInfo.readFields(in);
    if (isPartitionMessage()) {
      partitionID = in.readInt();
    }
    else if (isVertexMessage()) {
      vertexValues.readFields(in);
    }
  }

  @Override
  public TransmissionType getTransmissionType() {
    return transmissionType;
  }
  
  public void setTransmissionType(IControlMessage.TransmissionType transmissionType) {
    this.transmissionType = transmissionType;
  }
  
  public void setPartitionID(int partitionID) {
    this.setPartitionID(partitionID);
  }
  
  public void setVertexValues(String vertex) {
    this.vertexValues = new Text(vertex);
  }
  
  public void setextraInfo(String info) {
    this.generalInfo = new Text(info);
  }
  
  public String getExtraInfo() {
    return generalInfo.toString();
  }
  
  public boolean isNormalMessage() {
    return transmissionType == IControlMessage.TransmissionType.NORMAL;
  }
  public boolean isPartitionMessage() {
    return transmissionType == IControlMessage.TransmissionType.PARTITION;
  }
  
  public boolean isVertexMessage() {
    return transmissionType == IControlMessage.TransmissionType.VERTEX;
  }
  
  public boolean isBroadcastMessage() {
    return transmissionType == IControlMessage.TransmissionType.BROADCAST;
  }

  @Override
  public String toString() {
    if(isPartitionMessage()) {
      return String.valueOf(partitionID);
    }
    else if (isVertexMessage()) {
      return vertexValues.toString();
    }
    else {
      return null;
    }
  }
  
}
