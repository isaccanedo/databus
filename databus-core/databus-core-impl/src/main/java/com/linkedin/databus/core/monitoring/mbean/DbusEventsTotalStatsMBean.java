package com.linkedin.databus.core.monitoring.mbean;
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


import com.linkedin.databus.core.monitoring.events.DbusEventsTotalStatsEvent;

/**
 * Collector/accessor for total outbound traffic statistics.
 * @author cbotev
 *
 */
public interface DbusEventsTotalStatsMBean extends DatabusMonitoringMBean<DbusEventsTotalStatsEvent>
{

  // ************** GETTERS *********************

  /** Obtains the number of data events scanned */
  long getNumDataEvents();

  /** Obtains size of data events scanned (metadata and payload) */
  long getSizeDataEvents();

  /** Obtains size of the payload of data events scanned */
  long getSizeDataEventsPayload();

  /** Obtains the number of data events that were streamed out after filtering */
  long getNumDataEventsFiltered();

  /** Obtains size of data events (metadata and payload) that were streamed out after filtering */
  long getSizeDataEventsFiltered();

  /** Obtains size of the payload of data events that were streamed out after filtering */
  long getSizeDataEventsPayloadFiltered();

  /** Obtains the number of peers clients that have ever connected */
  int getNumPeers();

  /** Obtains the minimum requested window scn in a /stream call */
  long getMinSeenWinScn();

  /** Obtains the maximum requested window scn in a /stream call */
  long getMaxSeenWinScn();

  /** Obtains the maximum requested window scn filtered in a /stream call */
  long getMaxFilteredWinScn();

  /** Obtains the number of system events streamed out */
  long getNumSysEvents();

  /** Obtains the size of system events streamed out (metadata and payload */
  long getSizeSysEvents();

  /** Obtains the number of events that were invalid */
  long getNumInvalidEvents();

  /** Obtains number of events with header error */
  long getNumHeaderErrEvents();

  /** Obtains number of events with payload error */
  long getNumPayloadErrEvents();

  /** Obtains minimum requested scn in memory */
  long getMinScn();

  /** Obtains maximum requested scn in memory */
  long getMaxScn();

  /** Obtains scn immediately preceding the minScn , not in the eventBuffer */
  long getPrevScn();

  /** Obtains time of last access in ms */
  long getTimeSinceLastAccess();

  /** Obtains buffer creation time */
  long getTimeSinceCreation();

  /** Obtains free space */
  long getFreeSpace() ;

  /** Obtains time diff between timestamp of first event and last event **/
  long getTimeSpan();

  /** Gets the number of msecs it took for maxScn event to be accessed */
  long getTimeLag();

  /** Gets max time lag across all buffers */
  long getMaxTimeLag();

  /** Gets the min time lag across all buffers */
  long getMinTimeLag();

  /** Obtains time elapsed between latest event in buffer and now */
  long getTimeSinceLastEvent();

  /** Obtains time lang between event generation and event appearing in the buffer in ms */
  long getLatencyEvent();

  /**
   * For an aggregated stats object, provides the minimum of getTimeSinceLastAccess() metrics across
   * all buffers.
   * For a single stats object, the call does not make sense, but is the same as getTimeSinceLastAccess().
   */
  long getMinTimeSinceLastAccess();

  /**
   * For an aggregated stats object, provides the maximum of getTimeSinceLastAccess() metrics across
   * all buffers.
   * For a single stats object, the call does not make sense, but is the same as getTimeSinceLastAccess().
   */
  long getMaxTimeSinceLastAccess();

  /**
   * For an aggregated stats object, provides the minimum of getTimeSinceLastEvent() metrics across
   * all buffers.
   * For a single stats object, the call does not make sense, but is the same as getTimeSinceLastEvent().
   */
  long getMinTimeSinceLastEvent();

  /**
   * For an aggregated stats object, provides the maximum of getTimeSinceLastEvent() metrics across
   * all buffers.
   * For a single stats object, the call does not make sense, but is the same as getTimeSinceLastEvent().
   */
  long getMaxTimeSinceLastEvent();

  /**
   * For an aggregated stats object, provides the minimum of getTimeSpan() metrics across
   * all buffers.
   * For a single stats object, the call does not make sense, but is the same as getTimeSpan().
   */
  long getMinTimeSpan();

  /**
   * For an aggregated stats object, provides the maximum of getTimeSpan() metrics across
   * all buffers.
   * For a single stats object, the call does not make sense, but is the same as getTimeSpan().
   */
  long getMaxTimeSpan();
  // ****************** MUTATORS *********************

  /** Resets the statistics. */
  @Override
  void reset();




}
