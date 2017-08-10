package org.com.packagebase;

import org.com.exception.DeSerizliationException;
import org.com.exception.SerizliationException;

public abstract class PackageBase {
	
	/**将package内容序列化后写入到I/O */
	public abstract byte[] serizlization() throws SerizliationException;
	
	/**从I/O中反序列化后生成package*/
	public abstract void deSerizliation(byte[] buffer) throws DeSerizliationException;

}
