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
import java.util.List;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.Writable;

public class Message<K extends Writable, M extends Writable> implements IMessage<K, M> {
  private IMessage.MessageType messageType;
  private K subgraphID;
  private int partitionID;
  private byte[] msg;
  
  
  Message(IMessage.MessageType messageType, int partitionID, byte[] msg) {
    this.messageType = messageType;
    this.partitionID = partitionID;
    this.msg = msg;
  }
  
  Message(IMessage.MessageType messageType, K subgraphID, byte[] msg) {
    this.messageType = messageType;
    this.subgraphID = subgraphID;
    this.msg = msg;
  }
  
  Message(IMessage.MessageType messageType,byte[] msg) {
    this.messageType = messageType;
    this.msg = msg;
  }

  @Override
  public in.dream_lab.goffish.IMessage.MessageType getMessageType() {
    return messageType;
  }

  @Override
  public K getSubgraphID() {
    return subgraphID;
  }

  @Override
  public M getMessage() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    // TODO Auto-generated method stub
    
  }

}
