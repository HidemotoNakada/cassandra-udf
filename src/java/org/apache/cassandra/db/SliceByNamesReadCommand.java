/*
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
package org.apache.cassandra.db;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.CassandraServer;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.utils.ByteBufferUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SliceByNamesReadCommand extends ReadCommand
{
	private static final Logger logger = LoggerFactory.getLogger(SliceByNamesReadCommand.class);

	public final NamesQueryFilter filter;
	public String udfString = null;
	
	
  public SliceByNamesReadCommand(String table, ByteBuffer key, ColumnParent column_parent, Collection<ByteBuffer> columnNames) 
  {
  	this(table, key, column_parent, columnNames, null);
  }

  public SliceByNamesReadCommand(String table, ByteBuffer key, QueryPath path, Collection<ByteBuffer> columnNames)
  {
  	this(table, key, path, columnNames, null);
  }
	
	public SliceByNamesReadCommand(String table, ByteBuffer key, ColumnParent column_parent, Collection<ByteBuffer> columnNames,
  		String udf)
    {
        this(table, key, new QueryPath(column_parent), columnNames, udf);
    }

  public SliceByNamesReadCommand(String table, ByteBuffer key, QueryPath path, Collection<ByteBuffer> columnNames, String udf)
    {
        super(table, key, path, CMD_TYPE_GET_SLICE_BY_NAMES);
        SortedSet s = new TreeSet<ByteBuffer>(getComparator());
        s.addAll(columnNames);
        this.filter = new NamesQueryFilter(s);
        this.udfString = udf;
    }

  public SliceByNamesReadCommand(String table, ByteBuffer key, QueryPath path, NamesQueryFilter filter) 
  {
  	this(table, key, path, filter, null);
  }
  
    public SliceByNamesReadCommand(String table, ByteBuffer key, QueryPath path, NamesQueryFilter filter, String udf)
    {
        super(table, key, path, CMD_TYPE_GET_SLICE_BY_NAMES);
        this.filter = filter;
        this.udfString = udf;
    }

    public ReadCommand copy()
    {
        ReadCommand readCommand= new SliceByNamesReadCommand(table, key, queryPath, filter, udfString);
        readCommand.setDigestQuery(isDigestQuery());
        return readCommand;
    }

    public Row getRow(Table table) 
    {
   	  logger.error("" + UTF8Type.instance.getString(key) + " : udf = " + udfString);
    	DecoratedKey dk = StorageService.getPartitioner().decorateKey(key);
      Row	row =  table.getRow(new QueryFilter(dk, queryPath, filter));


  		if (udfString == null)
  			return row;
  		
  		UDF udfObj = null;
  		try {
  			udfObj = UDFFactory.create(udfString);
  		} catch (Exception e) {
  			logger.error("failed to parse udf as json. fall back to the normal operation", e);
  		}
  		try {
  			return udfObj.modify(row, udfString);
  		} catch (Exception e) {
  			logger.error("failed to execute udf. fall back to the normal oepration", e);
  			return row;
  		}
  		
    }

  	
    public String toString()
    {
        return "SliceByNamesReadCommand(" +
               "table='" + table + '\'' +
               ", key=" + ByteBufferUtil.bytesToHex(key) +
               ", columnParent='" + queryPath + '\'' +
               ", filter=" + filter +
               ')';
    }

    public IDiskAtomFilter filter()
    {
        return filter;
    }
}

class SliceByNamesReadCommandSerializer implements IVersionedSerializer<ReadCommand>
{
    public void serialize(ReadCommand cmd, DataOutput dos, int version) throws IOException
    {
        SliceByNamesReadCommand command = (SliceByNamesReadCommand) cmd;
        dos.writeBoolean(command.isDigestQuery());
        dos.writeUTF(command.table);
        ByteBufferUtil.writeWithShortLength(command.key, dos);
        command.queryPath.serialize(dos);
        NamesQueryFilter.serializer.serialize(command.filter, dos, version);
        if (command.udfString == null) {
        	dos.writeBoolean(false);
        } else {
        	dos.writeBoolean(true);
        	dos.writeUTF(command.udfString);
        }
    }

    public SliceByNamesReadCommand deserialize(DataInput dis, int version) throws IOException
    {
        boolean isDigest = dis.readBoolean();
        String table = dis.readUTF();
        ByteBuffer key = ByteBufferUtil.readWithShortLength(dis);
        QueryPath columnParent = QueryPath.deserialize(dis);

        AbstractType<?> comparator = ColumnFamily.getComparatorFor(table, columnParent.columnFamilyName, columnParent.superColumnName);
        NamesQueryFilter filter = NamesQueryFilter.serializer.deserialize(dis, version, comparator);
        String udf = null;
        if (dis.readBoolean())
        	udf = dis.readUTF();
        
        SliceByNamesReadCommand command = new SliceByNamesReadCommand(table, key, columnParent, filter, udf);
        command.setDigestQuery(isDigest);
        return command;
    }

    public long serializedSize(ReadCommand cmd, int version)
    {
        TypeSizes sizes = TypeSizes.NATIVE;
        SliceByNamesReadCommand command = (SliceByNamesReadCommand) cmd;
        int size = sizes.sizeof(command.isDigestQuery());
        int keySize = command.key.remaining();

        size += sizes.sizeof(command.table);
        size += sizes.sizeof((short)keySize) + keySize;
        size += command.queryPath.serializedSize(sizes);
        size += NamesQueryFilter.serializer.serializedSize(command.filter, version);
        size += sizes.sizeof(true);
        if (command.udfString != null) 
          size += sizes.sizeof(command.udfString);
        return size;
    }
}
