package org.com.util;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

public class BinaryInputStream extends BinaryInput {
	
	private DataInput in;
	
	public BinaryInputStream(InputStream is)
	{
		in = new DataInputStream(is);
	}
	
	@Override
	public long readLong() throws IOException
	{
		return in.readLong();
	}
	
	@Override
	public int readInt() throws IOException
	{
		return in.readInt();
	}

	@Override
	public boolean readBoolean() throws IOException 
	{
		return in.readBoolean();
	}
	
	public byte[] readByte(int len) throws IOException
	{
		byte[] buffer = new byte[len];
		in.readFully(buffer, 0, len);
		return buffer;
	}
	
	/**
	 * 这里要添加字符的输入方法
	 */

}
