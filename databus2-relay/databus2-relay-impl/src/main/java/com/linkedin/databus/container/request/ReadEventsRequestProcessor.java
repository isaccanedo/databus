package com.linkedin.databus.container.request;
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



import java.io.IOException;
import java.security.spec.InvalidParameterSpecException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import com.linkedin.databus.container.netty.HttpRelay;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.CheckpointMult;
import com.linkedin.databus.core.DbusEventBufferBatchReadable;
import com.linkedin.databus.core.DbusEventBufferMult;
import com.linkedin.databus.core.DbusEventBufferMult.PhysicalPartitionKey;
import com.linkedin.databus.core.DbusEventFactory;
import com.linkedin.databus.core.Encoding;
import com.linkedin.databus.core.OffsetNotFoundException;
import com.linkedin.databus.core.ScnNotFoundException;
import com.linkedin.databus.core.StreamEventsResult;
import com.linkedin.databus.core.data_model.DatabusSubscription;
import com.linkedin.databus.core.data_model.LogicalSource;
import com.linkedin.databus.core.data_model.PhysicalPartition;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.monitoring.mbean.StatsCollectors;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.DatabusHttpHeaders;
import com.linkedin.databus2.core.container.monitoring.mbean.HttpStatisticsCollector;
import com.linkedin.databus2.core.container.request.DatabusRequest;
import com.linkedin.databus2.core.container.request.InvalidRequestParamValueException;
import com.linkedin.databus2.core.container.request.RequestProcessingException;
import com.linkedin.databus2.core.container.request.RequestProcessor;
import com.linkedin.databus2.core.filter.ConjunctionDbusFilter;
import com.linkedin.databus2.core.filter.DbusFilter;
import com.linkedin.databus2.core.filter.DbusKeyCompositeFilter;
import com.linkedin.databus2.core.filter.DbusKeyFilter;
import com.linkedin.databus2.core.filter.KeyFilterConfigJSONFactory;
import com.linkedin.databus2.core.filter.SourceDbusFilter;
import com.linkedin.databus2.schemas.SourceIdNameRegistry;

public class ReadEventsRequestProcessor implements RequestProcessor
{
  public static final String MODULE = ReadEventsRequestProcessor.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  public final static String COMMAND_NAME = "stream";
  public final static String CHECKPOINT_PARAM = "checkPoint";
  public final static String CHECKPOINT_PARAM_MULT = "checkPointMult";
  public final static String FETCH_SIZE_PARAM = "size";
  public final static String OUTPUT_FORMAT_PARAM = "output";
  public final static String SOURCES_PARAM = "sources";
  public final static String KEY_MIN_PARAM = "keyMin";
  public final static String KEY_MAX_PARAM = "keyMax";
  public final static String OFFSET_PARAM = "offset";
  public final static String JSON_FORMAT_STRING = "json";
  public final static String BINARY_FORMAT_STRING = "binary";
  public final static String PARTITION_INFO_STRING = "filters";
  public final static String STREAM_FROM_LATEST_SCN = "streamFromLatestScn";
  public final static String SUBS_PARAM = "subs";

  private final ExecutorService _executorService;
  private final DbusEventBufferMult _eventBuffer;
  private final HttpRelay _relay;

  public ReadEventsRequestProcessor(ExecutorService executorService,
                                    HttpRelay relay)
  {
    _executorService = executorService;
    _relay = relay;
    _eventBuffer = _relay.getEventBuffer();
  }

  @Override
  public ExecutorService getExecutorService()
  {
    return _executorService;
  }

  @Override
  public DatabusRequest process(DatabusRequest request) throws IOException,
      RequestProcessingException, DatabusException
  {
	boolean isDebug = LOG.isDebugEnabled();
    try {
      ObjectMapper objMapper = new ObjectMapper();
      String checkpointString = request.getParams().getProperty(CHECKPOINT_PARAM, null);
      String checkpointStringMult = request.getParams().getProperty(CHECKPOINT_PARAM_MULT, null);
      int fetchSize = request.getRequiredIntParam(FETCH_SIZE_PARAM);
      String formatStr = request.getRequiredStringParam(OUTPUT_FORMAT_PARAM);
      Encoding enc = Encoding.valueOf(formatStr.toUpperCase());
      String sourcesListStr = request.getParams().getProperty(SOURCES_PARAM, null);
      String subsStr = request.getParams().getProperty(SUBS_PARAM, null);
      String partitionInfoStr = request.getParams().getProperty(PARTITION_INFO_STRING);
      String streamFromLatestSCNStr = request.getParams().getProperty(STREAM_FROM_LATEST_SCN);
      String clientMaxEventVersionStr = request.getParams().getProperty(DatabusHttpHeaders.MAX_EVENT_VERSION);
      int clientEventVersion = (clientMaxEventVersionStr != null) ?
          Integer.parseInt(clientMaxEventVersionStr) : DbusEventFactory.DBUS_EVENT_V1;

      if (clientEventVersion < 0 || clientEventVersion == 1 || clientEventVersion > DbusEventFactory.DBUS_EVENT_V2)
      {
        throw new InvalidRequestParamValueException(COMMAND_NAME,
                                                    DatabusHttpHeaders.MAX_EVENT_VERSION,
                                                    clientMaxEventVersionStr);
      }

      if (null == sourcesListStr && null == subsStr)
      {
        throw new InvalidRequestParamValueException(COMMAND_NAME, SOURCES_PARAM + "|" + SUBS_PARAM, "null");
      }

      //TODO for now we separte the code paths to limit the impact on existing Databus 2 deployments (DDSDBUS-79)
      //We have to get rid of this eventually and have a single data path.
      boolean v2Mode = null == subsStr;

      DbusKeyCompositeFilter keyCompositeFilter = null;
      if ( null != partitionInfoStr)
      {
    	  try
    	  {
    		  Map<Long, DbusKeyFilter> fMap= KeyFilterConfigJSONFactory.parseSrcIdFilterConfigMap(partitionInfoStr);
    		  keyCompositeFilter = new DbusKeyCompositeFilter();
    		  keyCompositeFilter.setFilterMap(fMap);
    		  if ( isDebug)
    			  LOG.debug("keyCompositeFilter is :" + keyCompositeFilter);
    	  } catch ( Exception ex) {
    		  String msg = "Got exception while parsing partition Configs. PartitionInfo is:" + partitionInfoStr;
    		  LOG.error(msg, ex);
    		  throw new InvalidRequestParamValueException(COMMAND_NAME, PARTITION_INFO_STRING, partitionInfoStr);
    	  }
      }


      boolean streamFromLatestSCN = false;

      if ( null != streamFromLatestSCNStr)
      {
    	  streamFromLatestSCN = Boolean.valueOf(streamFromLatestSCNStr);
      }

      long start = System.currentTimeMillis();

      List<DatabusSubscription> subs = null;

      //parse source ids
      SourceIdNameRegistry srcRegistry = _relay.getSourcesIdNameRegistry();
      HashSet<Integer> sourceIds = new HashSet<Integer>();
      if (null != sourcesListStr)
      {
        String[] sourcesList = sourcesListStr.split(",");
        for (String sourceId: sourcesList)
        {
          try
          {
            Integer srcId = Integer.valueOf(sourceId);
            sourceIds.add(srcId);
          }
          catch (NumberFormatException nfe)
          {
            HttpStatisticsCollector globalHttpStatsCollector = _relay.getHttpStatisticsCollector();
            if (null != globalHttpStatsCollector) {
              globalHttpStatsCollector.registerInvalidStreamRequest();
            }
            throw new InvalidRequestParamValueException(COMMAND_NAME, SOURCES_PARAM, sourceId);
          }
        }
      }

      //process explicit subscriptions and generate respective logical partition filters
      NavigableSet<PhysicalPartitionKey> ppartKeys = null;
      if (null != subsStr)
      {
        List<DatabusSubscription.Builder> subsBuilder = null;
        subsBuilder = objMapper.readValue(subsStr,
                                          new TypeReference<List<DatabusSubscription.Builder>>(){});
        subs = new ArrayList<DatabusSubscription>(subsBuilder.size());
        for (DatabusSubscription.Builder subBuilder: subsBuilder)
        {
          subs.add(subBuilder.build());
        }

        ppartKeys = new TreeSet<PhysicalPartitionKey>();
        for (DatabusSubscription sub: subs)
        {
          PhysicalPartition ppart = sub.getPhysicalPartition();
          if (ppart.isAnyPartitionWildcard())
          {
            ppartKeys = _eventBuffer.getAllPhysicalPartitionKeys(); break;
          }
          else
          {
            ppartKeys.add(new PhysicalPartitionKey(ppart));
          }
        }
      }
      // TODO
      // The following if statement is a very conservative one just to make sure that there are
      // not some clients out there that send subs, but do not send checkpoint mult. It seems that
      // this was the case during development but never in production, so we should remove this
      // pretty soon (1/28/2013).
      // Need to make sure that we don't have tests that send requests in this form.
      if(subs != null && checkpointStringMult == null && checkpointString != null) {
        throw new RequestProcessingException("Both Subscriptions and CheckpointMult should be present");
      }

      //convert source ids into subscriptions
      if (null == subs) subs = new ArrayList<DatabusSubscription>();
      for (Integer srcId: sourceIds)
      {
        LogicalSource lsource = srcRegistry.getSource(srcId);
        if(lsource == null)
          throw new InvalidRequestParamValueException(COMMAND_NAME, SOURCES_PARAM, srcId.toString());
        if(isDebug)
          LOG.debug("registry returns " + lsource  + " for srcid="+ srcId);
        DatabusSubscription newSub = DatabusSubscription.createSimpleSourceSubscription(lsource);
        subs.add(newSub);
      }

      DbusFilter ppartFilters = null;
      if (subs.size() > 0)
      {
        try
        {
          ppartFilters = _eventBuffer.constructFilters(subs);
        }
        catch (DatabusException de)
        {
          throw new RequestProcessingException("unable to generate physical partitions filters:" +
                                               de.getMessage(),
                                               de);
        }
      }

      ConjunctionDbusFilter filters = new ConjunctionDbusFilter();

      // Source filter comes first
      if (v2Mode) filters.addFilter(new SourceDbusFilter(sourceIds));
      else if (null != ppartFilters) filters.addFilter(ppartFilters);

      /*
      // Key range filter comes next
      if ((keyMin >0) && (keyMax > 0))
      {
        filters.addFilter(new KeyRangeFilter(keyMin, keyMax));
      }
      */
      if ( null != keyCompositeFilter)
      {
    	  filters.addFilter(keyCompositeFilter);
      }

      // need to update registerStreamRequest to support Mult checkpoint TODO (DDSDBUS-80)
      // temp solution
      // 3 options:
      // 1. checkpointStringMult not null - generate checkpoint from it
      // 2. checkpointStringMult null, checkpointString not null - create empty CheckpointMult
      // and add create Checkpoint(checkpointString) and add it to cpMult;
      // 3 both are null - create empty CheckpointMult and add empty Checkpoint to it for each ppartition
      PhysicalPartition pPartition;

      Checkpoint cp = null;
      CheckpointMult cpMult = null;

      if(checkpointStringMult != null) {
        try {
          cpMult = new CheckpointMult(checkpointStringMult);
        } catch (InvalidParameterSpecException e) {
          LOG.error("Invalid CheckpointMult:" + checkpointStringMult, e);
          throw new InvalidRequestParamValueException("stream", "CheckpointMult", checkpointStringMult);
        }
      } else {
        // there is no checkpoint - create an empty one
        cpMult = new CheckpointMult();
        Iterator<Integer> it = sourceIds.iterator();
        while(it.hasNext()) {
          Integer srcId = it.next();
          pPartition = _eventBuffer.getPhysicalPartition(srcId);
          if(pPartition == null)
            throw new RequestProcessingException("unable to find physical partitions for source:" + srcId);

          if(checkpointString != null) {
            cp = new Checkpoint(checkpointString);
          } else {
            cp = new Checkpoint();
            cp.setFlexible();
          }
          cpMult.addCheckpoint(pPartition, cp);
        }
      }

      if (isDebug) LOG.debug("checkpointStringMult = " + checkpointStringMult +  ";singlecheckpointString="+ checkpointString + ";CPM="+cpMult);

      // If the client has not sent a cursor partition, then use the one we may have retained as a part
      // of the server context.
      if (cpMult.getCursorPartition() == null) {
        cpMult.setCursorPartition(request.getCursorPartition());
      }
      if (isDebug) {
        if (cpMult.getCursorPartition() != null) {
          LOG.debug("Using physical paritition cursor " + cpMult.getCursorPartition());
        }
      }

      // for registerStreamRequest we need a single Checkpoint (TODO - fix it) (DDSDBUS-81)
      if(cp==null) {
        Iterator<Integer> it = sourceIds.iterator();
        if (it.hasNext()) {
          Integer srcId = it.next();
          pPartition = _eventBuffer.getPhysicalPartition(srcId);
          cp = cpMult.getCheckpoint(pPartition);
        } else {
          cp = new Checkpoint();
          cp.setFlexible();
        }
      }

      if (null != checkpointString && isDebug)
        LOG.debug("About to stream from cp: " + checkpointString.toString());

      HttpStatisticsCollector globalHttpStatsCollector = _relay.getHttpStatisticsCollector();
      HttpStatisticsCollector connHttpStatsCollector = null;
      if (null != globalHttpStatsCollector)
      {
        connHttpStatsCollector = (HttpStatisticsCollector)request.getParams().get(globalHttpStatsCollector.getName());
      }

      if (null != globalHttpStatsCollector) globalHttpStatsCollector.registerStreamRequest(cp, sourceIds);

      StatsCollectors<DbusEventsStatisticsCollector> statsCollectors = _relay.getOutBoundStatsCollectors();

      try
      {


        DbusEventBufferBatchReadable bufRead = v2Mode
          ? _eventBuffer.getDbusEventBufferBatchReadable(sourceIds, cpMult, statsCollectors)
          : _eventBuffer.getDbusEventBufferBatchReadable(cpMult, ppartKeys, statsCollectors);

        int eventsRead = 0;
        int minPendingEventSize = 0;
        StreamEventsResult result = null;

        bufRead.setClientMaxEventVersion(clientEventVersion);

        if (v2Mode)
        {
          result = bufRead.streamEvents(streamFromLatestSCN, fetchSize,
                                            request.getResponseContent(), enc, filters);
          eventsRead = result.getNumEventsStreamed();
          minPendingEventSize = result.getSizeOfPendingEvent();
          if(isDebug) {
            LOG.debug("Process: streamed " + eventsRead + " from sources " +
                     Arrays.toString(sourceIds.toArray()));
            LOG.debug("CP=" + cpMult); //can be used for debugging to stream from a cp
          }
          //if (null != statsCollectors) statsCollectors.mergeStatsCollectors();
        }
        else
        {
          result = bufRead.streamEvents(streamFromLatestSCN, fetchSize,
                                            request.getResponseContent(), enc, filters);
          eventsRead = result.getNumEventsStreamed();
          minPendingEventSize = result.getSizeOfPendingEvent();
          if(isDebug)
            LOG.debug("Process: streamed " + eventsRead + " with subscriptions " + subs);
          cpMult = bufRead.getCheckpointMult();
          if (cpMult != null) {
            request.setCursorPartition(cpMult.getCursorPartition());
          }
        }

        if (eventsRead == 0 && minPendingEventSize > 0)
        {
          // Append a header to indicate to the client that we do have at least one event to
          // send, but it is too large to fit into client's offered buffer.
          request.getResponseContent().addMetadata(DatabusHttpHeaders.DATABUS_PENDING_EVENT_SIZE,
                                                   minPendingEventSize);
          LOG.debug("Returning 0 events but have pending event of size " + minPendingEventSize);
        }
      }
      catch (ScnNotFoundException snfe)
      {
        if (null != globalHttpStatsCollector) {
          globalHttpStatsCollector.registerScnNotFoundStreamResponse();
        }
        throw new RequestProcessingException(snfe);
      }
      catch (OffsetNotFoundException snfe)
      {
        LOG.error("OffsetNotFound", snfe);
        if (null != globalHttpStatsCollector) {
          globalHttpStatsCollector.registerScnNotFoundStreamResponse();
        }
        throw new RequestProcessingException(snfe);
      }

      /* FIXME snagaraj
      if (null != connEventsStatsCollector && null != globalEventsStatsCollector)
      {
        globalEventsStatsCollector.merge(connEventsStatsCollector);
        connEventsStatsCollector.reset();
      }
      */

      if (null != connHttpStatsCollector)
      {
        connHttpStatsCollector.registerStreamResponse(System.currentTimeMillis()-start);
        globalHttpStatsCollector.merge(connHttpStatsCollector);
        connHttpStatsCollector.reset();
      }
      else if (null != globalHttpStatsCollector)
      {
        globalHttpStatsCollector.registerStreamResponse(System.currentTimeMillis()-start);
      }
    }
    catch (InvalidRequestParamValueException e)
    {
      HttpStatisticsCollector globalHttpStatsCollector = _relay.getHttpStatisticsCollector();
      if (null != globalHttpStatsCollector) {
        globalHttpStatsCollector.registerInvalidStreamRequest();
      }
      throw e;
    }
    return request;
  }

}
