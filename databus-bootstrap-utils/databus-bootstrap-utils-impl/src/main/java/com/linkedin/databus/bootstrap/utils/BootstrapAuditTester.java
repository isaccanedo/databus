package com.linkedin.databus.bootstrap.utils;
/*
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
 */


import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Struct;
import java.sql.Timestamp;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.log4j.Logger;

import com.linkedin.databus.client.DbusEventAvroDecoder;
import com.linkedin.databus.core.DbusEventFactory;
import com.linkedin.databus.core.DbusEventV1Factory;
import com.linkedin.databus.core.DbusEventInternalReadable;
import com.linkedin.databus2.producers.EventCreationException;
import com.linkedin.databus2.producers.db.OracleAvroGenericEventFactory;
import com.linkedin.databus2.relay.OracleJarUtils;
import com.linkedin.databus2.schemas.utils.SchemaHelper;

public class BootstrapAuditTester

{
  public static final String MODULE = BootstrapAuditTester.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);
  public static final boolean _sDebug = LOG.isDebugEnabled();

  private Schema _schema;
  private final String _tableName;  // a.k.a. view name (e.g., "following.sy$following")
  private final ByteBuffer _buffer;
  private DbusEventInternalReadable _event;


  public BootstrapAuditTester(Schema schema, String tableName)
  {
    _schema = schema;
    _tableName = tableName;
    byte[] b = new byte[1024 * 1024];
    _buffer = ByteBuffer.wrap(b);
    DbusEventFactory eventFactory = new DbusEventV1Factory();
    _event = eventFactory.createReadOnlyDbusEventFromBuffer(_buffer, 0);
  }

  private boolean compareField(Field f, Object databaseFieldValue, Object avroField)
  {
    // NULL condition handled
    if (databaseFieldValue == avroField)
    {
      return true;
    }
    if (databaseFieldValue == null)
    {
      // avroField cannot also be null or first conditional would have triggered
      LOG.error("compareField error: " + " field=" + f.name() + " null databaseFieldValue but non-null avroField " );
      return false;
    }
    if (avroField == null)
    {
      // databaseFieldValue cannot also be null or first conditional would have triggered
      LOG.error("compareField error: " + " field=" + f.name() + " non-null databaseFieldValue but null avroField " );
      return false;
    }

    try
    {
      Schema fieldSchema = SchemaHelper.unwindUnionSchema(f);  // == f.schema() if f is not a union
      Type avroFieldType = fieldSchema.getType();

      if (_sDebug)
      {
        LOG.debug("Checking for type:" + avroFieldType + ", Field:" + f.name() +
                  ", Exp:" + databaseFieldValue + ", Got:" + avroField);
      }
      switch (avroFieldType)
      {
        case BOOLEAN:
          assertEquals(f.name(),databaseFieldValue,avroField );
          break;
        case BYTES:
          byte[] byteArr = null;
          if (databaseFieldValue instanceof Blob)
          {
            Blob b = (Blob) databaseFieldValue;
            byteArr = b.getBytes(1,(int) b.length());
          }
          else
          {
            byteArr = (byte[])databaseFieldValue;
          }
          assertEquals(f.name(), byteArr, avroField);
          break;
        case DOUBLE:
          assertEquals(f.name(), new Double(((Number)databaseFieldValue).doubleValue()), (avroField));
          break;
        case FLOAT:
          assertEquals(f.name(), new Float(((Number)databaseFieldValue).floatValue()), (avroField));
          break;
        case INT:
          assertEquals(f.name(), Integer.valueOf(((Number)databaseFieldValue).intValue()), (avroField));
          break;
        case LONG:
          if(databaseFieldValue instanceof Number)
          {
              long lvalue = ((Number) databaseFieldValue).longValue();
              assertEquals(f.name(),lvalue,((Long)avroField).longValue());
          }
          else if(databaseFieldValue instanceof Timestamp)
          {
              long time = ((Timestamp) databaseFieldValue).getTime();
              assertEquals(f.name(),time,((Long)avroField).longValue());
          }
          else if(databaseFieldValue instanceof Date)
          {
              long time = ((Date) databaseFieldValue).getTime();
              assertEquals(f.name(),time,((Long)avroField).longValue());
          }
          else
          {
            Class timestampClass = null, dateClass = null;
            try
            {
              timestampClass = OracleJarUtils.loadClass("oracle.sql.TIMESTAMP");
              dateClass = OracleJarUtils.loadClass("oracle.sql.DATE");
            }
            catch (Exception e)
            {
              String errMsg = "Cannot convert " + databaseFieldValue.getClass() +
                              " to long. Unable to get Oracle datatypes " + e.getMessage();
              LOG.error(errMsg);
              throw new EventCreationException(errMsg);
            }

            if (timestampClass.isInstance(databaseFieldValue))
            {
              try
              {
                Object tsc = timestampClass.cast(databaseFieldValue);
                Method dateValueMethod = timestampClass.getMethod("dateValue");
                Date dateValue = (Date) dateValueMethod.invoke(tsc);
                long time = dateValue.getTime();
                assertEquals(f.name(),time,((Long)avroField).longValue());
              }
              catch(Exception ex)
              {
                String errMsg = "SQLException reading oracle.sql.TIMESTAMP value for field " + f.name();
                LOG.error(errMsg);
                throw new RuntimeException(errMsg, ex);
              }
            }
            else if (dateClass.isInstance(databaseFieldValue))
            {
              try
              {
                Object dsc = dateClass.cast(databaseFieldValue);
                Method dateValueMethod = dateClass.getMethod("dateValue");
                Date dateValue = (Date) dateValueMethod.invoke(dsc);
                long time = dateValue.getTime();
                assertEquals(f.name(),time,((Long)avroField).longValue());
              }
              catch (Exception ex)
              {
                String errMsg = "SQLException reading oracle.sql.DATE value for field " + f.name();
                LOG.error(errMsg);
                throw new RuntimeException(errMsg, ex);
              }
            }
            else
            {
              String errMsg = "Cannot convert " + databaseFieldValue.getClass() + " to long for field " + f.name();
              LOG.error(errMsg);
              throw new RuntimeException();
            }
          }
          break;
        case STRING:
          if (databaseFieldValue instanceof Clob)
          {
            String text = null;

            try
            {
              text = OracleAvroGenericEventFactory.extractClobText((Clob)databaseFieldValue, f.name());
            }
            catch (EventCreationException ex)
            {
              LOG.error("compareField error: " + ex.getMessage(), ex);
            }
            assertEquals(f.name(), text, ((Utf8)avroField).toString());
          }
          else
          {
            String text = databaseFieldValue.toString();
            assertEquals(f.name(), text, ((Utf8)avroField).toString());
          }
          break;
        case NULL:
          assertNull(f.name(), databaseFieldValue);
          assertNull(f.name(), avroField);
          break;
        case ARRAY:
          GenericArray<GenericRecord> avroArray = (GenericArray<GenericRecord>)avroField;
          Schema elementSchema = fieldSchema.getElementType();
          Array array = (Array)databaseFieldValue;
          ResultSet arrayResultSet = array.getResultSet();
          int i = 0;

          while (arrayResultSet.next())
          {
            // Get the underlying structure from the database. Oracle returns the structure in the
            // second column of the array's ResultSet
            Struct struct = (Struct) arrayResultSet.getObject(2);
            Object[] attributes = struct.getAttributes();

            GenericRecord avroElement = avroArray.get(i++);

            // Iterate over the fields in the JSON array of fields.
            // We can read the structure elements only by position, not by field name, so we
            // have to use dbFieldPosition recorded in the schema definition.
            for (Field field : elementSchema.getFields())
            {
              int dbFieldPosition = Integer.valueOf(SchemaHelper.getMetaField(field, "dbFieldPosition"));
              Object dbFieldValue = attributes[dbFieldPosition];
              Object avroFieldValue = avroElement.get(field.name());
              compareField(field, dbFieldValue, avroFieldValue);
            }
          }
          break;
        case RECORD:
          assert(compareRecord(fieldSchema, (Struct) databaseFieldValue, (GenericRecord) avroField)) :
                 "comparison of Avro 'record' type failed";
          break;
        case ENUM:
        case FIXED:
        case MAP:
        case UNION:
        default:
          String msg = "Audit for these fields not yet implemented for: "  + fieldSchema.getName() +
                       ", Avro type: " + avroFieldType;
          LOG.error(msg);
          throw new RuntimeException(msg);
      }
    }
    catch (AssertionError err)
    {
      LOG.error("compareField error: " + err.getMessage() + " field= " + f.name());
      return false;
    }
    catch (ClassCastException ce)
    {
      LOG.error("compareField error: " + ce.getMessage() + " field=" + f.name(), ce);
      return false;
    }
    catch ( Exception ex)
    {
      LOG.error("compareField error: " + ex.getMessage() + " field=" + f.name(), ex);
      return false;
    }

    return true;
  }

  static void assertTrue(String msg, boolean condition)
  {
    if (!condition) throw new AssertionError(msg);
  }

  static void assertTrue(boolean condition)
  {
    if (!condition) throw new AssertionError();
  }

  static void assertNotNull(Object ptr)
  {
    if (null == ptr) throw new AssertionError("!= null expected");
  }

  static void assertNull(Object ptr)
  {
    assertNull("", ptr);
  }

  static void assertNull(String msg, Object ptr)
  {
    if (null == msg) msg = "";
    if (null != ptr) throw new AssertionError(msg + ": == null expected");
  }

  static void assertEquals(Object expected, Object found)
  {
    assertEquals("", expected, found);
  }

  static void assertEquals(String msg, Object expected, Object found)
  {
    if (null == msg) msg = "";
    if (null == expected)
    {
      if (null != found) throw new AssertionError(msg + ": expected: null; found: " + found);
    }
    else if (null == found)
    {
      throw new AssertionError(msg + " expected: " + expected + "; found: null");
    }
    else if (!expected.equals(found))
    {
      throw new AssertionError(msg + " expected: " + expected + "; found: " + found);
    }
  }

  public boolean compareRecord(Schema schema, Struct oracleRecord, GenericRecord avroRecord)
  throws SQLException
  {
    List<Field> fields = schema.getFields();
    Object[] structAttribs = oracleRecord.getAttributes();
    if ((structAttribs.length != fields.size()) || fields.size() == 0)
    {
      LOG.error("Num fields do not match: " + structAttribs.length + " : " + fields.size());
      return false;
    }
    for (Field avroField : fields)
    {
      String dbFieldPositionStr = SchemaHelper.getMetaField(avroField, "dbFieldPosition");
      int dbFieldPosition = 0;
      if (null != dbFieldPositionStr && !dbFieldPositionStr.isEmpty())
      {
        //two fields are extracted, then the entire table is projected
        dbFieldPosition = Integer.valueOf(dbFieldPositionStr) + 3;
      }
      else
      {
        LOG.error("Could not find dbFieldPosition for " + avroField.name());
        return false;
      }
      Object expObj  = structAttribs[dbFieldPosition];
      Object gotObj  = avroRecord.get(avroField.name());
      if (_sDebug)
      {
        LOG.debug("Key:" + avroField.name() + ",Got Object:" + gotObj);
      }

      if (!compareField(avroField,expObj, gotObj))
      {
        return false;
      }
    }
    return true;
  }

  public boolean compareRecord(Schema schema, ResultSet oracleRecord, GenericRecord avroRecord)
  throws SQLException
  {
    List<Field> fields = schema.getFields();
    boolean result = true;
    for (Field avroField : fields)
    {
      int dbFieldPosition = 0;
      // this is just avroField.schema() if avroField isn't a union; else schema of first non-null subtype:
      Schema fieldSchema = SchemaHelper.unwindUnionSchema(avroField);
      Type avroFieldType = fieldSchema.getType();

      String dbFieldPositionStr = SchemaHelper.getMetaField(avroField, "dbFieldPosition");
      if (avroFieldType == Type.ARRAY)
      {
        if (null == dbFieldPositionStr || dbFieldPositionStr.isEmpty())
        {
          Schema elementSchema = fieldSchema.getElementType();
          dbFieldPositionStr = SchemaHelper.getMetaField(elementSchema, "dbFieldPosition");
        }
      }
      if (null != dbFieldPositionStr && !dbFieldPositionStr.isEmpty())
      {
        //two fields are extracted, then the entire table is projected
        dbFieldPosition = Integer.valueOf(dbFieldPositionStr) + 3;
      }
      else
      {
        LOG.error("compareRecord: Could not find dbFieldPosition for " + avroField.name());
        return false;
      }
      Object expObj = null;
      try
      {
        expObj = oracleRecord.getObject(dbFieldPosition);
      }
      catch (SQLException sx)
      {
        // expand on ambiguous "java.sql.SQLException: Invalid column index" message:
        // (sudo -uapp bin/run-audit-meta.sh ei following)
        String errMsg = "SQLException reading object for avroField " + avroField.name() +
                        " at dbFieldPosition " + dbFieldPositionStr + "+3: maybe view " + _tableName +
                        " used in " + BootstrapSeederMain.getSourcesConfigFile() +
                        " doesn't match that in schema " + _schema.getName() + "?";
        LOG.error(errMsg);
        throw new RuntimeException(errMsg, sx);
      }
      Object gotObj = avroRecord.get(avroField.name());
      if (_sDebug)
      {
        LOG.debug("Key:" + avroField.name() + ", got object:" + gotObj);
      }
      if (!compareField(avroField, expObj, gotObj))
      {
        result = false;
      }
    }
    return result;
  }

  public boolean compareRecord(ResultSet expRs, GenericRecord avroRec)
  throws SQLException
  {
    return compareRecord(_schema, expRs, avroRec);
  }

  public boolean compareRecord(ResultSet expRs, ResultSet avroFormattedRs, DbusEventAvroDecoder decoder)
  throws SQLException
  {
    if (_sDebug)
    {
      LOG.debug("Compare Record:");
    }
    GenericRecord record = getGenericRecord(avroFormattedRs, decoder);

    return compareRecord(_schema, expRs, record);
  }

  public GenericRecord getGenericRecord(ResultSet avroFormattedRs,DbusEventAvroDecoder decoder)
  throws SQLException
  {
    _buffer.clear();
    _buffer.put(avroFormattedRs.getBytes("val"));
    _event = _event.reset(_buffer, 0);
    GenericRecord record = decoder.getGenericRecord(_event);
    return record;
  }

}
