package org.com.test;

public class StringToByteTest {
	public static void main(String[] args)
	{
		String s = "wyblpwan";
		byte[] buffer = s.getBytes();
		System.out.println(buffer.length);
		System.out.println(s.length());
	}
}
