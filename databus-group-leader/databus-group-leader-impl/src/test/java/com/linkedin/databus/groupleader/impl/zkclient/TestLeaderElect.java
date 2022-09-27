package com.linkedin.databus.groupleader.impl.zkclient;
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


import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.File;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus.core.util.FileUtils;
import com.linkedin.databus.core.util.Utils;
import com.linkedin.databus.groupleader.pub.AcceptLeadershipCallback;
import com.linkedin.databus.groupleader.pub.GroupLeadershipConnection;
import com.linkedin.databus.groupleader.pub.GroupLeadershipConnectionFactory;
import com.linkedin.databus.groupleader.pub.GroupLeadershipSession;
import com.linkedin.databus2.test.TestUtil;

/*
 * IMPORTANT NOTE : If you add any new test case methods, make sure you wrap it with LockObject.
 * This class is NOT thread-safe without it but TestNg expects the test class to be thread-safe.
 */
public class TestLeaderElect
{
	private static final Logger LOG = Logger.getLogger(TestLeaderElect.class);

	public static final Object LockObject = new Object();

	public static String getRequiredStringProperty(String propname)
	{
		String val = System.getProperty(propname);

		if (val == null)
		{
			throw new IllegalArgumentException("Missing property: " + propname);
		}

		LOG.info("Property " + propname + "=" + val);
		return val;
	}

	public static int getRequiredIntProperty(String propname)
	{
		String stringVal = getRequiredStringProperty(propname);
		int intVal = Integer.parseInt(stringVal);
		return intVal;
	}

	public static boolean getRequiredBooleanProperty(String propname)
	{
		String stringVal = getRequiredStringProperty(propname);
		boolean booleanVal = Boolean.parseBoolean(stringVal);
		return booleanVal;
	}

	protected boolean _startLocalZookeeper;
	protected String _zkTestDataParentDir;
	protected String _zkTestDataRootDir;
	protected String _zkServerList;
	protected int _sessionTimeoutMillis;
	protected int _connectTimeoutMillis;
	protected int _zkServerTickTime = 2000;

	protected List<ZkServer> _localZkServers;
	protected GroupLeadershipConnectionFactory _groupLeadershipConnFactory;
	protected GroupLeadershipConnection _adminLeadershipConn;
	protected ZkClient _adminZkClient;

	@BeforeClass
	public void classSetup()
	{
	  TestUtil.setupLoggingWithTimestampedFile(true, "/tmp/TestLeaderElect_", ".log",
	                                           Level.INFO);
	}

	protected void setUp() throws Exception
	{
		//temporary

		System.setProperty("startLocalZookeeper","true");
		//.setProperty("zkServerList","localhost:2191,localhost:3192,localhost:4193");
		final int zkPort = Utils.getAvailablePort(2191);
        File zkroot = FileUtils.createTempDir("TestLeaderElect_zkroot");
        LOG.info("starting ZK on port " + zkPort + " and datadir " + zkroot.getAbsolutePath());

		System.setProperty("zkServerList","localhost:" + zkPort);

		System.setProperty("sessionTimeoutMillis","10000");
		System.setProperty("connectTimeoutMillis","5000");

		System.setProperty("zkTestDataParentDir", zkroot.getAbsolutePath());

		_startLocalZookeeper = getRequiredBooleanProperty("startLocalZookeeper");
		_zkTestDataParentDir = getRequiredStringProperty("zkTestDataParentDir");
		_zkServerList =  getRequiredStringProperty("zkServerList");
		_sessionTimeoutMillis = getRequiredIntProperty("sessionTimeoutMillis");
		_connectTimeoutMillis = getRequiredIntProperty("connectTimeoutMillis");


		if (_startLocalZookeeper)
		{
			_zkTestDataParentDir = _zkTestDataParentDir.trim();

			if (_zkTestDataParentDir.length() == 0)
			{
				throw new IllegalArgumentException("You must specify a valid zkTestDataParentDir when startLocalZookeeper=true");
			}

			File zkTestDataParentDirFile = new File(_zkTestDataParentDir);
			if (!zkTestDataParentDirFile.exists())
			{
				throw new IllegalArgumentException("Specified zkTestDataParentDir does not exist: " + _zkTestDataParentDir);
			}

			_zkTestDataRootDir = _zkTestDataParentDir + "/zkroot";

			List<Integer> localPortsList = LeaderElectUtils.parseLocalPorts(_zkServerList);


			if (localPortsList.size() > 1)
			{
				throw new IllegalArgumentException("Currently we only suppport starting ONE local zookeeper server");
			}

			_localZkServers = LeaderElectUtils.startLocalZookeeper(localPortsList, _zkTestDataRootDir, _zkServerTickTime);
		}

		_groupLeadershipConnFactory = new GroupLeadershipConnectionFactoryZkClientImpl(
				_zkServerList,
				_sessionTimeoutMillis,
				_connectTimeoutMillis);

		_adminLeadershipConn = _groupLeadershipConnFactory.getConnection();

		_adminZkClient = ((GroupLeadershipConnectionZkClientImpl) _adminLeadershipConn).getZkClient();

	}

    protected void tearDown() throws Exception
    {
      if (null != _adminLeadershipConn)
      {
        _adminLeadershipConn.close();
      }

      if (_startLocalZookeeper && null != _localZkServers)
      {
        LeaderElectUtils.stopLocalZookeeper(_localZkServers);
      }
    }

	protected String cleanZkGroupInfo(String zkBasePath, String groupName)
	{
		String groupNodePath = GroupLeadershipConnectionZkClientImpl.makeGroupNodePath(zkBasePath,groupName);
		_adminZkClient.deleteRecursive(groupNodePath);
		return groupNodePath;
	}

	/*
	 * IMPORTANT NOTE : If you add any new test case methods, make sure you wrap it with LockObject.
	 * This class is NOT thread-safe without it but TestNg expects the test class to be thread-safe.
	 */
	@Test
	public void testLeaderElectSimple() throws Exception
	{
		synchronized(LockObject)
		{
			try
			{
				setUp();
				String baseName = "/databus2.testGroupLeader";
				String groupName = "TestLeaderElect.testLeaderElectSimple";
				cleanZkGroupInfo(baseName,groupName);

				final AtomicReference<String> callerMaintainedCurrentLeaderRef = new AtomicReference<String>();

				AcceptLeadershipCallback saveCurrentLeaderCallback =
						new AcceptLeadershipCallback()
				{
					@Override
					public void doAcceptLeadership(GroupLeadershipSession groupLeadershipSession)
					{
						callerMaintainedCurrentLeaderRef.set(groupLeadershipSession.getMemberName());
					}
				};

				GroupLeadershipConnection conn002 = _groupLeadershipConnFactory.getConnection();
				Assert.assertNull(conn002.getLeaderName(baseName,groupName),"Leader should be null");
				LOG.info("Group should not exist yet Test");
				assertFalse(conn002.getGroupNames(baseName).contains(groupName));

				GroupLeadershipSession sess002 = conn002.joinGroup(baseName,
						groupName, "002", saveCurrentLeaderCallback);

				assertEquals(groupName, sess002.getGroupName());
				assertEquals("002", sess002.getMemberName());
				assertEquals("002", (sess002.getLeaderName()));
				LOG.info("002 should be leader Test");
				assertFalse( ! sess002.isLeader());
				assertEquals("002", callerMaintainedCurrentLeaderRef.get());
				LOG.info("Group should exist Test");
				assertFalse(! conn002.getGroupNames(baseName).contains(groupName));
				assertEquals(groupName, conn002.getGroupLeadershipInfo(baseName,groupName).getGroupName());
				assertEquals("002", (conn002.getGroupLeadershipInfo(baseName,groupName).getLeaderName()));
				assertEquals(1, conn002.getGroupLeadershipInfo(baseName,groupName).getMemberNames().size());
				System.err.printf("membership info=%s\n", conn002.getGroupLeadershipInfo(baseName, groupName).getMemberNames());
				LOG.info("Member should be in group Test");
				assertFalse(!conn002.getGroupLeadershipInfo(baseName,groupName).getMemberNames().contains("002"));



				GroupLeadershipConnection conn001 = _groupLeadershipConnFactory.getConnection();

				GroupLeadershipSession sess001 = conn001.joinGroup(baseName,
						groupName, "001", saveCurrentLeaderCallback);

				assertEquals(groupName, sess001.getGroupName());
				assertEquals("001", sess001.getMemberName());
				assertEquals("002", (sess001.getLeaderName()));
				LOG.info("001 should not be leader");
				assertFalse(sess001.isLeader());
				assertEquals("002", callerMaintainedCurrentLeaderRef.get());

				LOG.info("Group should exist");
				assertFalse(! conn001.getGroupNames(baseName).contains(groupName));
				assertEquals(groupName, conn001.getGroupLeadershipInfo(baseName,groupName).getGroupName());
				assertEquals("002", (conn001.getGroupLeadershipInfo(baseName,groupName).getLeaderName()));
				assertEquals(2, conn001.getGroupLeadershipInfo(baseName,groupName).getMemberNames().size());
				LOG.info("Member should be in group");
				assertFalse(! conn001.getGroupLeadershipInfo(baseName,groupName).getMemberNames().contains("001"));
				LOG.info("Member should be in group");
				assertFalse(!conn001.getGroupLeadershipInfo(baseName,groupName).getMemberNames().contains("002"));



				GroupLeadershipConnection conn003 = _groupLeadershipConnFactory.getConnection();

				GroupLeadershipSession sess003 = conn003.joinGroup(baseName,
						groupName, "003", saveCurrentLeaderCallback);

				assertEquals(groupName, sess003.getGroupName());
				assertEquals("003", sess003.getMemberName());
				assertEquals("002", (sess003.getLeaderName()));
				LOG.info("003 should not be leader");
				assertFalse( sess003.isLeader());
				assertEquals("002", callerMaintainedCurrentLeaderRef.get());

				LOG.info("Group should exist");
				assertTrue(conn003.getGroupNames(baseName).contains(groupName));
				assertEquals(groupName, conn003.getGroupLeadershipInfo(baseName,groupName).getGroupName());
				assertEquals("002", (conn003.getGroupLeadershipInfo(baseName,groupName).getLeaderName()));
				assertEquals(3, conn003.getGroupLeadershipInfo(baseName,groupName).getMemberNames().size());
				LOG.info("Member should be in group");
				assertTrue(conn003.getGroupLeadershipInfo(baseName,groupName).getMemberNames().contains("001"));
				assertTrue(conn003.getGroupLeadershipInfo(baseName,groupName).getMemberNames().contains("002"));
				assertTrue(conn003.getGroupLeadershipInfo(baseName,groupName).getMemberNames().contains("003"));

				sess002.leaveGroup();

				for (int i = 0; i < 60; i++)
				{

					if ("001".equals((_adminLeadershipConn.getGroupLeadershipInfo(baseName,groupName).getLeaderName())))
					{
						break;
					}
					Thread.sleep(500);
				}

				assertEquals(groupName, sess001.getGroupName());
				assertEquals("001", sess001.getMemberName());
				assertEquals("001", (sess001.getLeaderName()));
				LOG.info("001 should be leader");
				assertTrue(sess001.isLeader());
				assertEquals("001", callerMaintainedCurrentLeaderRef.get());

				LOG.info("Group should exist");
				assertTrue(_adminLeadershipConn.getGroupNames(baseName).contains(groupName));
				assertEquals(groupName, _adminLeadershipConn.getGroupLeadershipInfo(baseName,groupName).getGroupName());
				assertEquals("001",(_adminLeadershipConn.getGroupLeadershipInfo(baseName,groupName).getLeaderName()));
				assertEquals(2, _adminLeadershipConn.getGroupLeadershipInfo(baseName,groupName).getMemberNames().size());
				LOG.info("Member should be in group");
				assertTrue(_adminLeadershipConn.getGroupLeadershipInfo(baseName,groupName).getMemberNames().contains("001"));
				assertTrue(_adminLeadershipConn.getGroupLeadershipInfo(baseName,groupName).getMemberNames().contains("003"));

				assertEquals(groupName, sess003.getGroupName());
				assertEquals("003", sess003.getMemberName());
				assertEquals("001", sess003.getLeaderName());
				LOG.info("003 should not be leader");
				assertFalse(sess003.isLeader());

				try
				{
					sess002.leaveGroup();
					fail("Should have gotten IllegalStateException");
				}
				catch (IllegalStateException e)
				{
					// expected
				}

				sess003.leaveGroup();
				sess001.leaveGroup();
				conn003.close();
				conn002.close();
				conn001.close();
			} finally {
				tearDown();
			}
		}
	}
}
