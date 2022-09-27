package com.linkedin.databus2.core.container.request;
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


import java.nio.ByteOrder;

public class BinaryProtocol
{
   // Protocol constants

  /** Byte order for number serialization */
  public static final ByteOrder BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;
  public static final byte RECOVERABLE_ERROR_THRESHOLD = (byte)0;
  public static final byte UNRECOVERABLE_ERROR_THRESHOLD = (byte)-64;

  /** Maximum number of bytes of the error class name to be serialized*/
  public static final int MAX_ERROR_CLASS_LEN = 100;

  /** Maximum number of bytes of the error message to be serialized*/
  public static final int MAX_ERROR_MESSAGE_LEN = 1000;

  /** Result ok */
  public static final byte RESULT_OK = 0;

  /* Recoverable errors */
  /** Server found itself in an unexpected state while processing the request */
  public static final byte RESULT_ERR_INTERNAL_SERVER_ERROR = RECOVERABLE_ERROR_THRESHOLD - 1;

  /**
   * The producer is trying to push events for sources which may generate gaps in the event
   * sequence. */
  public static final byte RESULT_ERR_SOURCES_TOO_OLD = RECOVERABLE_ERROR_THRESHOLD - 2;

  /**
   * The producer is trying to push events for an unknown partition */
  public static final byte RESULT_ERR_UNKNOWN_PARTITION = RECOVERABLE_ERROR_THRESHOLD - 3;

  /* Unrecoverable errors */
  /** Server received unknown command */
  public static final byte RESULT_ERR_UNKNOWN_COMMAND = UNRECOVERABLE_ERROR_THRESHOLD - 1;

  /** The requested protocol version is not supported */
  public static final byte RESULT_ERR_UNSUPPORTED_PROTOCOL_VERSION = UNRECOVERABLE_ERROR_THRESHOLD - 2;

  /**
   * The server received unexpected command; generally seen when the server expects certain
   * commands in a fixed sequence. */
  public static final byte RESULT_ERR_UNEXPECTED_COMMAND = UNRECOVERABLE_ERROR_THRESHOLD - 3;

  /** One of the request parameters had invalid value */
  public static final byte RESULT_ERR_INVALID_REQ_PARAM = UNRECOVERABLE_ERROR_THRESHOLD - 4;

  /** Control event before any data event*/
  public static final byte RESULT_ERR_UNEXPECTED_CONTROL_EVENT = UNRECOVERABLE_ERROR_THRESHOLD - 5;

  /** Invalid sequence number in the event */
  public static final byte RESULT_ERR_INVALID_EVENT = UNRECOVERABLE_ERROR_THRESHOLD - 6;

  /** Unsupported DbusEventVersion */
  public static final byte RESULT_ERR_UNSUPPORTED_DBUS_EVENT_VERSION = UNRECOVERABLE_ERROR_THRESHOLD - 7;

  /**
   * We received metadata with mismatching versions.
   * Marking this unrecoverable, since metadata versions are decided at startSendEvents time,
   * and we can change parameters only if a new connection is established.
   */
  public static final byte RESULT_ERR_INVALID_METADATA = UNRECOVERABLE_ERROR_THRESHOLD - 8;

  public enum ErrorType
  {
    NONE,
    RECOVERABLE,
    UNRECOVERABLE,
  }

  public static ErrorType getErrorType(byte code)
  {
    if (code == RESULT_OK)
    {
      return ErrorType.NONE;
    }
    if (code < UNRECOVERABLE_ERROR_THRESHOLD)
    {
      return ErrorType.UNRECOVERABLE;
    }
    return ErrorType.RECOVERABLE;
  }
}
