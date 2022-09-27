package com.linkedin.databus.core;
/*
 *
 * Copyright 2013 LinkedIn Corp. All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/


import java.nio.channels.WritableByteChannel;

import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.filter.DbusFilter;

/**
 * allows to read events from the buffer in a batch (of some fixed size)
 */
public interface DbusEventBufferBatchReadable
{
  /**
  * Modifies the CheckpointMult contained in the object.
   *
   * @param streamFromLatestScn
   * @param batchFetchSize
   * @param writeChannel
   * @param encoding
   * @param filter
   * @return
  * @throws ScnNotFoundException
  */

 public StreamEventsResult streamEvents(boolean streamFromLatestScn,
                                        int batchFetchSize,
                                        WritableByteChannel writeChannel,
                                        Encoding encoding,
                                        DbusFilter filter)
 throws ScnNotFoundException, DatabusException, OffsetNotFoundException;

  /**
   * @return CheckpointMult containing the current checkpoints.
   */
 public CheckpointMult getCheckpointMult();

 /**
  * specify max DbusEvent version client supports
  */
 public void setClientMaxEventVersion(int version);
}
