package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.io.*;

import org.apache.cassandra.db.marshal.UTF8Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ExternalProcessUDF extends UDF {
	private static final Logger logger = LoggerFactory.getLogger(ExternalProcessUDF.class);
	
	String [] commands; 
	public ExternalProcessUDF() {
		
	}
			
	public void setArgs(Map argMap) throws Exception{
		List<Object> l = (List<Object>) argMap.get("commands");
		if (l == null)
			throw new Exception("cannot find commands.");
		commands = new String[l.size()];
		int i = 0;
		for (Object o: l)  
			commands[i++] = (String)o; 
		
	}
	
	
	
  @Override
  public ByteBuffer processEach(ByteBuffer val) throws Exception {
  	ProcessBuilder pb = new ProcessBuilder();
  	pb.command(commands);
  	Process p = pb.start(); 
  	OutputStream os = p.getOutputStream();
  	InputStream is = p.getInputStream();
  	
  	os.write(val.array(), val.limit(), val.limit() - val.position());
  	os.close();
  	logger.error("buf = " + val);
  	
  	ByteArrayOutputStream bos = new ByteArrayOutputStream();
  	final int length = 100000;
  	byte [] buf = new byte[length];
  	int read = 0;
  	int offset = 0;
  	while ((read = is.read(buf, 0, length)) > 0) 
  		bos.write(buf, offset, read);
  	
  	return ByteBuffer.wrap(bos.toByteArray());
  }
}
