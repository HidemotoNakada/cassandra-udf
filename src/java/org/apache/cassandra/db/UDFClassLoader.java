package org.apache.cassandra.db;

import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.io.IOException;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UDFClassLoader extends ClassLoader {
	private static final Logger logger = LoggerFactory.getLogger(UDFClassLoader.class);
	String jarPath;
	JarFile jarFile;
	
	public UDFClassLoader(String jarPath) {
		this.jarPath = jarPath;
	}

  public Class findClass(String name) throws ClassNotFoundException {
    byte[] b = null;
    try {
    	b = loadClassData(name);
      return defineClass(name, b, 0, b.length);
    } catch (java.lang.ClassFormatError e){
      System.out.print(name+":blen = " + b.length);
      for (int i = 0; i < 10; i++){
       System.out.print( (int)b[b.length - 10 + i] + " ");
      }
      System.out.println("");
      e.printStackTrace();
      throw new ClassNotFoundException();
    }
  }

  private byte[] loadClassData(String name) throws ClassNotFoundException{
  	name = name.replace('.', '/') + ".class";
  	try {
			if (jarFile == null)
				jarFile = new JarFile(jarPath);
			JarEntry jarEntry = jarFile.getJarEntry(name);
			if (jarEntry == null)
				throw new ClassNotFoundException(jarPath + " does not include class " + name);
			InputStream is = jarFile.getInputStream(jarEntry);

			int length = (int)jarEntry.getSize();
			byte [] buffer = new byte[length];
			int read = 0;
			while (read < length){
				int tmp = is.read(buffer, read, length - read);
				read += tmp;
			}
			return buffer;
		} catch (IOException e) {
			logger.error("failed to load jar", e);
			throw new ClassNotFoundException(e.getMessage());
		}
  }

}
