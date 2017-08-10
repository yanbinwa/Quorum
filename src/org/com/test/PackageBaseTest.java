package org.com.test;

import org.com.exception.DeSerizliationException;
import org.com.exception.SerizliationException;
import org.com.packagebase.SelectPackage;

public class PackageBaseTest {
	public static void main(String[] args) throws SerizliationException, DeSerizliationException
	{
		SelectPackage sp = new SelectPackage(1, 1, 1, 1, 1, 1, 1, 1, 1);
		
		System.out.println(sp);
		
		byte[] buf = sp.serizlization();
		
		SelectPackage sp1 = new SelectPackage();
		
		sp1.deSerizliation(buf);
		
		System.out.println(sp1);
	}
}
