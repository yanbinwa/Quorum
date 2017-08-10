package org.com.data;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.com.constant.QuorumConstant;
import org.com.exception.DeSerizliationException;
import org.com.exception.SerizliationException;
import org.com.packagebase.PackageBase;
import org.com.util.BinaryInputStream;
import org.com.util.BinaryOutputStream;

/**
 * 这里的变量支持int，long，String三种
 */

public class Value extends PackageBase {

	int valueType = -1;
	Object value = null;
	
	public Object getValue()
	{
		return value;
	}
	
	@Override
	public byte[] serizlization() throws SerizliationException 
	{
		// TODO Auto-generated method stub
		ByteArrayOutputStream baos = null;
		try 
		{
			baos = new ByteArrayOutputStream();
			BinaryOutputStream bos = new BinaryOutputStream(baos);
			bos.writeInt(valueType);
			switch(valueType)
			{
			case QuorumConstant.VALUE_INT:
				Integer val = (Integer)value;
				bos.writeInt(val);
				break;
			
			case QuorumConstant.VALUE_LONG:
				Long val1 = (Long)value;
				bos.writeLong(val1);
				break;
			
			case QuorumConstant.VALUE_STINRG:
				String val2 = (String)value;
				bos.writeInt(val2.length());
				bos.writeByte(val2.getBytes());
				break;
			}
		} 
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return baos.toByteArray();
	}
	
	@Override
	public void deSerizliation(byte[] buffer) throws DeSerizliationException 
	{
		try 
		{
		// TODO Auto-generated method stub
			ByteArrayInputStream bios = new ByteArrayInputStream(buffer);
			BinaryInputStream bis = new BinaryInputStream(bios);
			valueType = bis.readInt();
			switch(valueType)
			{
			case QuorumConstant.VALUE_INT:
				int val = bis.readInt();
				value = new Integer(val);
				break;
			
			case QuorumConstant.VALUE_LONG:
				long val1 = bis.readLong();
				value = new Long(val1);
				break;
			
			case QuorumConstant.VALUE_STINRG:
				int valLen = bis.readInt();
				byte[] buf = bis.readByte(valLen);
				value = new String(buf);
				break;
			}
		} 
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
