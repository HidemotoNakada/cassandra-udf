package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.db.marshal.UTF8Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class UDF {
	private static final Logger logger = LoggerFactory.getLogger(SliceByNamesReadCommand.class);
		
	public Row modify(Row row, String udf) {
		ColumnFamily newCf = ColumnFamily.create(row.cf.id());
		Map<String, ByteBuffer> map = new HashMap<String, ByteBuffer>();
		for (ByteBuffer colName: row.cf.getColumnNames()) {
			IColumn col = row.cf.getColumn(colName);
			ByteBuffer val = col.value();
			map.put(UTF8Type.instance.getString(colName), val);
		}
		try {
			Map<String, ByteBuffer> processed =  process(map);
			for (String key: processed.keySet()) {
				long timestamp = 0;
				ByteBuffer colNameBB = ByteBuffer.wrap(key.getBytes()); 
				newCf.addColumn(new Column(colNameBB, processed.get(key), timestamp));  
			}
		} catch (Exception e) {
			logger.error("execption UDF", e);
		}
		
		Row newRow = new Row(row.key, newCf);
		return newRow;
	}
	
	
  public Map<String, ByteBuffer> process(Map<String, ByteBuffer> map) throws Exception{
  	Map<String, ByteBuffer> newMap = new HashMap<String, ByteBuffer>();
  	for (String key: map.keySet()) 
  		newMap.put(key, processEach(map.get(key)));
  	
  	return newMap;
  }

  public abstract ByteBuffer processEach(ByteBuffer val) throws Exception;


	public void setArgs(Map argMap) throws Exception {
		//ignore
	}

	
	
	
}
