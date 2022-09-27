/**
 *
 */
package com.linkedin.databus.bootstrap.server;
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
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import com.linkedin.databus.bootstrap.common.BootstrapDBMetaDataDAO;
import com.linkedin.databus.bootstrap.common.BootstrapDBMetaDataDAO.SourceStatusInfo;
import com.linkedin.databus.bootstrap.common.BootstrapHttpStatsCollector;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus2.core.container.request.BootstrapDatabaseTooOldException;
import com.linkedin.databus2.core.container.request.BootstrapDatabaseTooYoungException;
import com.linkedin.databus2.core.container.request.DatabusRequest;
import com.linkedin.databus2.core.container.request.RequestProcessingException;

public class StartSCNRequestProcessor extends BootstrapRequestProcessorBase
{
  public static final String MODULE = StartSCNRequestProcessor.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  public final static String COMMAND_NAME = "startSCN";
  public final static String SOURCES_PARAM = "sources";
  public final static String SOURCE_DELIMITER = ",";

  public StartSCNRequestProcessor(ExecutorService executorService,
		  						  BootstrapServerStaticConfig config,
                                  BootstrapHttpServer bootstrapServer)
     throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException
  {
	  super(executorService,config,bootstrapServer);
  }


  @Override
  protected DatabusRequest doProcess(DatabusRequest request) throws IOException,
      RequestProcessingException
  {
    BootstrapHttpStatsCollector bootstrapStatsCollector = _bootstrapServer.getBootstrapStatsCollector();
    long startTime = System.currentTimeMillis();
    String sources = request.getRequiredStringParam(SOURCES_PARAM);
    List<String> srcList =  getSources(sources);

	Checkpoint ckpt = new Checkpoint(request.getRequiredStringParam(CHECKPOINT_PARAM));

    LOG.info("StartSCN requested for sources : (" + sources + "). CheckPoint is :" + ckpt);
    long sinceScn = ckpt.getBootstrapSinceScn();
    ObjectMapper mapper = new ObjectMapper();
    StringWriter out = new StringWriter(1024);
    long startSCN = -1;
    BootstrapSCNProcessor processor = null;

    try
    {
    	processor = new BootstrapSCNProcessor(_config, _bootstrapServer.getInboundEventStatisticsCollector());
    	List<SourceStatusInfo> srcStatusPairs = null;
    	try
    	{
    		srcStatusPairs = processor.getSourceIdAndStatusFromName(srcList);
    		startSCN = processor.getMinApplierWindowScn(sinceScn, srcStatusPairs);
    		if (processor.shouldBypassSnapshot(sinceScn, startSCN, srcStatusPairs))
    		{
    			LOG.info("Bootstrap Snapshot phase will be bypassed for startScn request :" + request);
    			LOG.info("Original startSCN is:" + startSCN + ", Setting startSCN to the sinceSCN:" + sinceScn);
    			startSCN = sinceScn;
    		}
    		else
    		{
    		  if (startSCN == BootstrapDBMetaDataDAO.DEFAULT_WINDOWSCN)
    		  {
    		    throw new RequestProcessingException("Bootstrap DB is being initialized! startSCN=" + startSCN);
    		  }

    		  if (_config.isEnableMinScnCheck())
    		  {
    		    //snapshot isn't bypassed. Check if snapshot is possible from sinceScn by checking minScn
    		    long minScn = processor.getBootstrapMetaDataDAO().getMinScnOfSnapshots(srcStatusPairs);
    		    LOG.info("Min scn for tab tables is: " + minScn);
    		    if (minScn == BootstrapDBMetaDataDAO.DEFAULT_WINDOWSCN)
    		    {
    		      throw new BootstrapDatabaseTooYoungException("BootstrapDB has no minScn for these sources, but minScn check is enabled! minScn=" + minScn);
    		    }

            //Note: The cleaner deletes rows less than or equal to scn: BootstrapDBCleaner::doClean
    		    //sinceSCN should be greater than minScn, unless sinceScn=minScn=0
    		    if ((sinceScn <= minScn) && !(sinceScn==0 && minScn==0))
    		    {
    		      LOG.error("Bootstrap Snapshot doesn't have requested data . sinceScn too old! sinceScn is " + sinceScn +  " but minScn available is " + minScn);
    		      throw new BootstrapDatabaseTooYoungException("Min scn=" + minScn + " Since scn=" + sinceScn);
    		    }
    		  }
    		  else
    		  {
    		    LOG.debug("Bypassing minScn check! ");
    		  }
        }
    	}
    	catch (BootstrapDatabaseTooOldException tooOldException)
    	{
    		if (bootstrapStatsCollector != null)
    		{
    			bootstrapStatsCollector.registerErrStartSCN();
    			bootstrapStatsCollector.registerErrDatabaseTooOld();
    		}

    		LOG.error("The bootstrap database is too old!", tooOldException);
    		throw new RequestProcessingException(tooOldException);
    	}
    	catch (BootstrapDatabaseTooYoungException e)
    	{
    	  if (bootstrapStatsCollector != null)
        {
          bootstrapStatsCollector.registerErrStartSCN();
          bootstrapStatsCollector.registerErrBootstrap();
        }
        LOG.error("The bootstrap database is too young!", e);
        throw new RequestProcessingException(e);
    	}
    	catch (SQLException e)
    	{
    	    if (bootstrapStatsCollector != null)
    	    {
    	    	bootstrapStatsCollector.registerErrStartSCN();
    	        bootstrapStatsCollector.registerErrSqlException();
    	    }
    		LOG.error("Error encountered while fetching startSCN from database.", e);
    		throw new RequestProcessingException(e);
    	}
    	mapper.writeValue(out, String.valueOf(startSCN));
    	byte[] resultBytes = out.toString().getBytes(Charset.defaultCharset());
    	request.getResponseContent().write(ByteBuffer.wrap(resultBytes));
    	LOG.info("startSCN: " + startSCN + " with server Info :" + _serverHostPort);

    } catch (RequestProcessingException ex) {
      LOG.error("Got exception while calculating startSCN", ex);
      throw ex;
    } catch (Exception ex) {
    	LOG.error("Got exception while calculating startSCN", ex);
    	throw new RequestProcessingException(ex);
    } finally {
    	if ( null != processor)
    		processor.shutdown();
    }

    if (bootstrapStatsCollector != null)
    {
        bootstrapStatsCollector.registerStartSCNReq(System.currentTimeMillis()-startTime);
    }
    return request;
  }

  private List<String> getSources(String sources)
  {
	  List<String> srcList = new ArrayList<String>();

	  if ( null == sources )
	  {
		  return null;
	  }

	  String[] sourceList = sources.split(SOURCE_DELIMITER);
	  for(String s : sourceList)
	  {
		  srcList.add(s);
	  }

  	  return srcList;
  }
}
