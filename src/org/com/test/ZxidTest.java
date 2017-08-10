package org.com.test;

public class ZxidTest {
	
	static public long makeZxid(long epoch, long counter) 
	{
		return (epoch << 32L) | (counter & 0xffffffffL);
	}
	public static void main(String[] args)
	{
		int i = 1;
		long j = i;
		long l = j << 32L;
		System.out.println(l);
	}
}
