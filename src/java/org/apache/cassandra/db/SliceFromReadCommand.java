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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.service.RowDataResolver;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.utils.ByteBufferUtil;

public class SliceFromReadCommand extends ReadCommand
{
	static final Logger logger = LoggerFactory.getLogger(SliceFromReadCommand.class);

    public final SliceQueryFilter filter;
    public String udfString = null;
    
    public SliceFromReadCommand(String table, ByteBuffer key, ColumnParent column_parent, ByteBuffer start, ByteBuffer finish, boolean reversed, int count)
    {
        this(table, key, new QueryPath(column_parent), start, finish, reversed, count);
    }

    public SliceFromReadCommand(String table, ByteBuffer key, QueryPath path, ByteBuffer start, ByteBuffer finish, boolean reversed, int count)
    {
        this(table, key, path, new SliceQueryFilter(start, finish, reversed, count));
    }

    public SliceFromReadCommand(String table, ByteBuffer key, QueryPath path, SliceQueryFilter filter)
    {
        super(table, key, path, CMD_TYPE_GET_SLICE);
        this.filter = filter;
    }
    public SliceFromReadCommand(String table, ByteBuffer key, ColumnParent column_parent, ByteBuffer start, ByteBuffer finish, boolean reversed, int count, String udfString)
    {
        this(table, key, new QueryPath(column_parent), start, finish, reversed, count, udfString);
    }

    public SliceFromReadCommand(String table, ByteBuffer key, QueryPath path, ByteBuffer start, ByteBuffer finish, boolean reversed, int count, String udfString)
    {
        this(table, key, path, new SliceQueryFilter(start, finish, reversed, count), udfString);
    }

    public SliceFromReadCommand(String table, ByteBuffer key, QueryPath path, SliceQueryFilter filter, String udfString)
    {
        super(table, key, path, CMD_TYPE_GET_SLICE);
        this.filter = filter;
        this.udfString = udfString;
    }

    public ReadCommand copy()
    {
        ReadCommand readCommand = new SliceFromReadCommand(table, key, queryPath, filter);
        readCommand.setDigestQuery(isDigestQuery());
        return readCommand;
    }

    public Row getRow(Table table)
    {
   	  logger.error("" + UTF8Type.instance.getString(key) + " : udf = " + udfString);
   	  DecoratedKey dk = StorageService.getPartitioner().decorateKey(key);
        Row row =  table.getRow(new QueryFilter(dk, queryPath, filter));

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

    @Override
    public ReadCommand maybeGenerateRetryCommand(RowDataResolver resolver, Row row)
    {
        int maxLiveColumns = resolver.getMaxLiveCount();

        int count = filter.count;
        // We generate a retry if at least one node reply with count live columns but after merge we have less
        // than the total number of column we are interested in (which may be < count on a retry).
        // So in particular, if no host returned count live columns, we know it's not a short read.
        if (maxLiveColumns < count)
            return null;

        int liveCountInRow = row == null || row.cf == null ? 0 : filter.getLiveCount(row.cf);
        if (liveCountInRow < getOriginalRequestedCount())
        {
            // We asked t (= count) live columns and got l (=liveCountInRow) ones.
            // From that, we can estimate that on this row, for x requested
            // columns, only l/t end up live after reconciliation. So for next
            // round we want to ask x column so that x * (l/t) == t, i.e. x = t^2/l.
            int retryCount = liveCountInRow == 0 ? count + 1 : ((count * count) / liveCountInRow) + 1;
            SliceQueryFilter newFilter = filter.withUpdatedCount(retryCount);
            return new RetriedSliceFromReadCommand(table, key, queryPath, newFilter, getOriginalRequestedCount());
        }

        return null;
    }

    @Override
    public void maybeTrim(Row row)
    {
        if ((row == null) || (row.cf == null))
            return;

        filter.trim(row.cf, getOriginalRequestedCount());
    }

    public IDiskAtomFilter filter()
    {
        return filter;
    }

    /**
     * The original number of columns requested by the user.
     * This can be different from count when the slice command is a retry (see
     * RetriedSliceFromReadCommand)
     */
    protected int getOriginalRequestedCount()
    {
        return filter.count;
    }

    @Override
    public String toString()
    {
        return "SliceFromReadCommand(" +
               "table='" + table + '\'' +
               ", key='" + ByteBufferUtil.bytesToHex(key) + '\'' +
               ", column_parent='" + queryPath + '\'' +
               ", filter='" + filter + '\'' +
               ')';
    }
}

class SliceFromReadCommandSerializer implements IVersionedSerializer<ReadCommand>
{
    public void serialize(ReadCommand rm, DataOutput dos, int version) throws IOException
    {
        SliceFromReadCommand realRM = (SliceFromReadCommand)rm;
        dos.writeBoolean(realRM.isDigestQuery());
        dos.writeUTF(realRM.table);
        ByteBufferUtil.writeWithShortLength(realRM.key, dos);
        realRM.queryPath.serialize(dos);
        SliceQueryFilter.serializer.serialize(realRM.filter, dos, version);
        if ((realRM.udfString) == null) {
        	dos.writeBoolean(false);
        } else {
        	dos.writeBoolean(true);
        	dos.writeUTF(realRM.udfString);
        }
    }

    public ReadCommand deserialize(DataInput dis, int version) throws IOException
    {
        boolean isDigest = dis.readBoolean();
        String table = dis.readUTF();
        ByteBuffer key = ByteBufferUtil.readWithShortLength(dis);
        QueryPath path = QueryPath.deserialize(dis);
        SliceQueryFilter filter = SliceQueryFilter.serializer.deserialize(dis, version);
        String udf = null;
        if (dis.readBoolean()) 
        	udf = dis.readUTF();
        
        SliceFromReadCommand rm = new SliceFromReadCommand(table, key, path, filter, udf);
        rm.setDigestQuery(isDigest);
        return rm;
    }

    public long serializedSize(ReadCommand cmd, int version)
    {
        TypeSizes sizes = TypeSizes.NATIVE;
        SliceFromReadCommand command = (SliceFromReadCommand) cmd;
        int keySize = command.key.remaining();

        int size = sizes.sizeof(cmd.isDigestQuery()); // boolean
        size += sizes.sizeof(command.table);
        size += sizes.sizeof((short) keySize) + keySize;
        size += command.queryPath.serializedSize(sizes);
        size += SliceQueryFilter.serializer.serializedSize(command.filter, version);
        size += sizes.sizeof(true);
        if (command.udfString != null) 
          size += sizes.sizeof(command.udfString);        
        return size;
    }
}
