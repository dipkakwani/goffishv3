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

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import in.dream_lab.goffish.humus.api.IControlMessage;

public class ControlMessage implements IControlMessage{

  private IControlMessage.TransmissionType transmissionType;
  private int numOfValues = 0;
  private final ByteArrayOutputStream byteBuffer = new ByteArrayOutputStream();
  
  public ControlMessage() {
    transmissionType = IControlMessage.TransmissionType.NORMAL;
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeEnum(out, transmissionType);
    out.writeInt(numOfValues);
    out.writeInt(byteBuffer.size());
    out.write(byteBuffer.toByteArray());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    transmissionType = WritableUtils.readEnum(in, IControlMessage.TransmissionType.class);
    this.numOfValues = in.readInt();
    int bytesLength = in.readInt();
    byte[] temp = new byte[bytesLength];
    in.readFully(temp);
    byteBuffer.write(temp);
  }
  
  public void add(byte[] value) {
    try {
      byteBuffer.write(value);
      numOfValues++;
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  public void addValuesBytes(byte[] values, int numOfValues) {
    try {
      byteBuffer.write(values);
      this.numOfValues += numOfValues;
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public TransmissionType getTransmissionType() {
    return transmissionType;
  }
  
  public void setTransmissionType(IControlMessage.TransmissionType transmissionType) {
    this.transmissionType = transmissionType;
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
    return byteBuffer.toString();
  }
  
}
