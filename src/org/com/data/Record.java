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

/*
 * 操作类型（ADD，UPDATE, DELETE）
 * 
 * ADD: tag + 路径长度 + 路径 + 变量类型 + 变量数值 (解析后变量类型，就可以调用相应的方法来获取变量值了)
 * UPDATE: tag + 路径长度 + 路径 + 变量类型 + 变量数值
 * DELETE: tag + 路径长度 + 路径
 * 
 * 这里的变量支持int，long，String三种
 * 
 */

public class Record extends PackageBase {

	int tag;
	String path;
	Value value= null;	
	
	public String toString()
	{
		String ret = String.format("tag is %d, path is %s", tag, path);
		return ret;
	}
	
	@Override
	public byte[] serizlization() throws SerizliationException {
		// TODO Auto-generated method stub
		ByteArrayOutputStream baos = null;
		try 
		{
			/**DELETE*/
			baos = new ByteArrayOutputStream();
			BinaryOutputStream bos = new BinaryOutputStream(baos);
			bos.writeInt(tag);
			bos.writeInt(path.length());
			bos.writeByte(path.getBytes());
			switch(tag)
			{
			case QuorumConstant.RECORD_ADD:
			case QuorumConstant.RECORD_UPDATE:
				byte[] buf = value.serizlization();
				bos.writeInt(buf.length);
				bos.writeByte(buf);
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
	public void deSerizliation(byte[] buffer) throws DeSerizliationException {
		// TODO Auto-generated method stub
		try
		{
			ByteArrayInputStream bios = new ByteArrayInputStream(buffer);
			BinaryInputStream bis = new BinaryInputStream(bios);
			tag = bis.readInt();
			int pathLeng = bis.readInt();
			byte[] buf = bis.readByte(pathLeng);
			path = new String(buf);
			switch(tag)
			{
			case QuorumConstant.RECORD_ADD:
			case QuorumConstant.RECORD_UPDATE:
				value = new Value();
				int valueLen = bis.readInt();
				byte[] buf1 = bis.readByte(valueLen);
				value.deSerizliation(buf1);
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
