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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class Message<K extends Writable, M extends Writable> implements IMessage<K, M> {
  private IMessage.MessageType messageType;
  private K subgraphID;
  //private int partitionID;
  private M message;
  private IControlMessage control;
  
  Message() {
    this.messageType = IMessage.MessageType.CUSTOM_MESSAGE;
  }
  
  Message(IMessage.MessageType messageType, K subgraphID, M msg) {
    this.messageType = messageType;
    this.subgraphID = subgraphID;
    this.message = msg;
  }
  
  Message(IMessage.MessageType messageType, M msg) {
    this.messageType = messageType;
    this.message = msg;
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
    return null;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    subgraphID.write(out);
    message.write(out);
    WritableUtils.writeEnum(out, messageType);
    control.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    subgraphID.readFields(in);
    message.readFields(in);
    messageType = WritableUtils.readEnum(in, IMessage.MessageType.class);
    control.readFields(in);
  }
}
