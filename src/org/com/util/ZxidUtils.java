package org.com.util;

public class ZxidUtils 
{
	static public long getEpochFromZxid(long zxid) 
	{
		return zxid >> 32L;
	}
	static public long getCounterFromZxid(long zxid) 
	{
		return zxid & 0xffffffffL;
	}
	static public long makeZxid(int epoch, long counter) 
	{
		long tmp = epoch;
		return (tmp << 32L) | (counter & 0xffffffffL);
	}
}
