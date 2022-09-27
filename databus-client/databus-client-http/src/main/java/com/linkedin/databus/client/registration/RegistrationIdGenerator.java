package com.linkedin.databus.client.registration;
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


import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.codec.binary.Hex;
import org.apache.log4j.Logger;

import com.linkedin.databus.client.pub.RegistrationId;
import com.linkedin.databus.core.data_model.DatabusSubscription;

public class RegistrationIdGenerator
{
    public static final Logger LOG = Logger.getLogger(RegistrationIdGenerator.class);

	/**
	 * Database of Ids for registrations created so far
	 */
	private static Set<String> _registrationIdDatabase;
	static {
		_registrationIdDatabase = new HashSet<String>();
	}

	/**
	 * Queries the existing database of registrations inside the client and generates a new id different from
	 * any of them
	 *
	 * @param prefix : A String prefix which can be specified to precede the id to be generated. Typically this
	 *                 is the name of the Consumer class
	 * @param subsSources : List of subscriptions the consumer is interested in
	 * @return RegistrationId : Generated based on the prefix _ 8 byte md5 hash ( subscriptions ) _ count
	 */
	public static RegistrationId generateNewId(String prefix, Collection<DatabusSubscription> subsSources)
	{
		StringBuilder subscription = new StringBuilder();
		for (DatabusSubscription ds : subsSources)
		{
			if (ds != null)
				subscription.append(ds.generateSubscriptionString());
		}

		String id = generateUniqueString(prefix, subscription.toString());
		RegistrationId rid = new RegistrationId(id);
		return rid;
	}

	/**
	 * For a given regId , this API queries the existing database of registrations inside the client.
	 * If the id is available, it is returned as a registration id, otherwise a running counter 
	 * is appended to the suggested regId.
	 * 
	 * @param id : A suggested name by the caller to be used as regId. If the id is not available, a running counter
	 *             is appended 
	 * @return RegistrationId : Generated based on the id _count
	 */
	public static RegistrationId generateNewId(String id)
	{
		String r = generateUniqueString(id);
		RegistrationId rid = new RegistrationId(r);
		return rid;
	}

	/**
	 * Checks if the input rid can be used as a RegistrationId.
	 * Specifically, checks to see if the underlying id is already in use
	 * @return
	 */
	public static boolean isIdValid(RegistrationId rid)
	{
		String id = rid.getId();
		synchronized (RegistrationIdGenerator.class)
		{
    		if (_registrationIdDatabase.contains(id))
    		{
    			return false;
    		}
    		else
    		{
    			return true;
    		}
		}
	}

	/**
	 * Adds an id into the RegistrationId database.
	 * This is useful for unit-testing / inserting an id out-of-band into the database, so that such an id
	 * would not get generated again
	 *
	 * @param id
	 */
	public static void insertId(RegistrationId rid)
	{
		String id = rid.getId();
		synchronized (RegistrationIdGenerator.class)
		{
		  _registrationIdDatabase.add(id);
		}
	}

	/**
	 * Creates a unique string given a prefix ( which must appear as is ), a subscription string ( which is converted
	 * to an 8 byte string ) and a count if there are duplicates with the previous two
	 */
	private static String generateUniqueString(String prefix, String subscription)
	{
		final String delimiter = "_";
		String baseId = prefix + delimiter + generateByteHash(subscription);
		
		return generateUniqueString(baseId);
	}
	
	/**
	 * Creates a unique registration Id string given an id. 
	 * A running counter is appended if there are duplicates 
	 */
	private static String generateUniqueString(String id)
	{
		final String delimiter = "_";
		String baseId = id;
		
		boolean success = false;
		boolean debugEnabled = LOG.isDebugEnabled();
		synchronized (RegistrationIdGenerator.class)
        {
	        int count = _registrationIdDatabase.size();
		    while (! success)
    		{
    			if (_registrationIdDatabase.contains(id))
    			{
    				if (debugEnabled)
    				  LOG.debug("The generated id " + id + " already exists. Retrying ...");
                    id = baseId + delimiter + count;
    				count++;
    			}
    			else
    			{
    				if (debugEnabled)
    				  LOG.debug("Obtained a new ID " + id);
    				_registrationIdDatabase.add(id);
    				success = true;
    			}
    		}
        }
		return id;
	}

	/**
	 * Generate a hash out of the String id
	 *
	 * @param id
	 * @return
	 */
	private static String generateByteHash(String id)
	{
		try {
			final MessageDigest messageDigest = MessageDigest.getInstance("MD5");
			messageDigest.reset();
			messageDigest.update(id.getBytes(Charset.forName("UTF8")));
			final byte[] resultsByte = messageDigest.digest();
			String hash = new String(Hex.encodeHex(resultsByte));

			final int length = 8;
			if (hash.length() > length)
				hash = hash.substring(0, length);

			return hash;
		}
		catch (NoSuchAlgorithmException nse)
		{
			LOG.error("Unexpected error : Got NoSuchAlgorithm exception for MD5" );
			return "";
		}
	}

}
