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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class Message<K extends Writable, M extends Writable> implements IMessage<K, M> {
  private IMessage.MessageType messageType;
  private K subgraphID;
  //private int partitionID;
  private boolean hasSubgraphID;
  private boolean hasMessage;
  
  private M message;
  private IControlMessage control;
  
  Message() {
    this.messageType = IMessage.MessageType.CUSTOM_MESSAGE;
    this.hasSubgraphID = false;
    this.hasMessage = false;
    control = new ControlMessage();
  }
  
  Message(IMessage.MessageType messageType, M msg) {
    this();
    this.messageType = messageType;
    this.message = msg;
    this.hasMessage = true;
  }
  
  Message(IMessage.MessageType messageType, K subgraphID, M msg) {
    this(messageType, msg);
    this.subgraphID = subgraphID;
    this.hasSubgraphID = true;
  }
  
  
  
  public void setControlInfo(IControlMessage controlMessage) {
    this.control = controlMessage;
  }
  
  public IControlMessage getControlInfo() {
    return control;
  }
  
  @Override
  public in.dream_lab.goffish.IMessage.MessageType getMessageType() {
    return messageType;
  }
  
  public void setMessageType(IMessage.MessageType messageType) {
    this.messageType = messageType;
  }

  @Override
  public K getSubgraphID() {
    return subgraphID;
  }

  @Override
  public M getMessage() {
    return message;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    control.write(out);
    WritableUtils.writeEnum(out, messageType);
    out.writeBoolean(hasSubgraphID);
    if (hasSubgraphID) {
      subgraphID.write(out);
    }
    out.writeBoolean(hasMessage);
    if (hasMessage) {
      message.write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    control = new ControlMessage(); 
    control.readFields(in);
    messageType = WritableUtils.readEnum(in, IMessage.MessageType.class);
    hasSubgraphID = in.readBoolean();
    if (hasSubgraphID) {
      subgraphID.readFields(in);
    }
    hasMessage = in.readBoolean();
    if (hasMessage) {
      //use reflectionUtils to instatiate message
      this.message = (M)new LongWritable();
      message.readFields(in);
    }    
  }
}
