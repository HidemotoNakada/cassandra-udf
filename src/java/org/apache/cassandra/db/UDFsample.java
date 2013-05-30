package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.db.marshal.UTF8Type;

public class UDFsample extends UDF {

  @Override
  public ByteBuffer processEach(ByteBuffer val) {
  	String str = UTF8Type.instance.getString(val);
  	return UTF8Type.instance.fromString(str + " mod");
  }
  
  

/*
  public static String toString(ByteBuffer buffer) {
		if (buffer == null)
			return null;
		try {
			buffer.mark();
			byte[] bytes = new byte[buffer.remaining()];
			buffer.get(bytes);
			buffer.reset();
			return new String(bytes, "UTF-8");
		} catch (Exception e) {
			return "";
		}
	}
	*/
}
