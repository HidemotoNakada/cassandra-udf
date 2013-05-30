package org.apache.cassandra.db;

import java.lang.reflect.Field;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class JsonUtil {
	public static class JsonUtilException extends Exception {
		public JsonUtilException(String s) {
			super(s);
		}
		public JsonUtilException(Throwable t) {
			super(t);
		}
	}
	
	public static Object unmarshal(Object obj, Class clazz) throws JsonUtilException {
		try {
			if (clazz == String.class)
				return (String)obj;
		if (clazz == Integer.class)
			return (Integer)obj;
		if (clazz == Long.class)
			return (Long)obj;
		if (clazz == List.class) 
			return (JSONArray)obj;
		
		JSONObject jObj = (JSONObject)obj;
		Object o = clazz.newInstance();
		
		for (Object s: jObj.keySet()) {
			Field f =  clazz.getField((String)s);
			if (!f.isAccessible())
				continue;
			Object inner = unmarshal(jObj.get(s), f.getType());
			f.set(o, inner);
		}
		return o;
		
	} catch (Exception e) {
		throw new JsonUtilException(e);
	}
	}
}
