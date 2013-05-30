package org.apache.cassandra.db;

import java.util.Map;

import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;

public class UDFFactory {
	
	static UDF create(String udfString) throws JsonUtil.JsonUtilException {
   	JSONParser parser = new JSONParser();
  	Map argMap;
   	try {
			argMap = (Map)parser.parse(udfString);
		} catch (ParseException e) {
			throw new JsonUtil.JsonUtilException(e);
		}
   	String classname = (String)argMap.get("classname");
		if (classname == null)
			throw new JsonUtil.JsonUtilException("no classname found in json " + udfString);
		
		
		try {
			String jarName = (String)argMap.get("jar");
			Class clazz;
			if (jarName != null) { 
				UDFClassLoader udfClassLoader = new UDFClassLoader(jarName);
				clazz = udfClassLoader.findClass(classname);
			} else 
				clazz = Class.forName(classname);
			
			Object o = clazz.newInstance();
			UDF udf = (UDF)o;
			udf.setArgs(argMap);
			return udf;
		} catch (Exception e) {
			throw new JsonUtil.JsonUtilException(e);
		}
		
	}
}
