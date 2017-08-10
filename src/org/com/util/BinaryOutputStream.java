package org.com.util;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class BinaryOutputStream extends BinaryOutput {
	
	private DataOutput out = null;
	
	public BinaryOutputStream(OutputStream os)
	{
		out = new DataOutputStream(os);
	}
	
	@Override
	public void writeLong(long value) throws IOException
	{
		out.writeLong(value);
	}
	
	@Override
	public void writeInt(int value) throws IOException
	{
		out.writeInt(value);
	}

	@Override
	public void writeBoolean(boolean value) throws IOException 
	{
		out.writeBoolean(value);
	}
	
	public void writeByte(byte[] buffer) throws IOException
	{
		out.write(buffer);
	}
	
	/**
	 * 这里要添加字符的输出方法
	 */
}
