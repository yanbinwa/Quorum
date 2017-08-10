package org.com.quorum;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * QuorumConfig负责读取Quorum的配置信息，server ip，port，
 * 
 */

public class QuorumConfig 
{
	
	//String QUORUM_CONFIG = "/Users/yanbinwa/Documents/workspace/Quorum/doc/quorum.conf";
	String QUORUM_CONFIG = "/opt/quorum/config/quorum.conf";
	String MYID = "serverId";
	String SELECT_PORT = "selectPort";
	String CLIENT_PORT = "clientPort";
	String SYNC_PORT = "syncPort";
	String SERVER_IP = "serverIp";
	String DELIMITER_IP = ",";
	
	public Map<Integer, String> ips = null;
	public int myId;
	public int selectPort;
	public int clientPort;
	public int syncPort;
	
	public QuorumConfig() {}
	
	public void loadConfig() 
	{
		Properties prop = new Properties();
		try 
		{
			FileInputStream in = new FileInputStream(QUORUM_CONFIG);
			prop.load(in);
		} 
		catch (FileNotFoundException e) 
		{
			System.out.println(e.getMessage());
		} 
		catch (IOException e) 
		{
			System.out.println(e.getMessage());
		}
		
		ips = new HashMap<Integer, String>();
		myId = Integer.parseInt(prop.getProperty(MYID));
		selectPort = Integer.parseInt(prop.getProperty(SELECT_PORT));
		clientPort = Integer.parseInt(prop.getProperty(CLIENT_PORT));
		syncPort = Integer.parseInt(prop.getProperty(SYNC_PORT));
		
		String serverIps = prop.getProperty(SERVER_IP);
		if(serverIps != null) 
		{
			String[] ipList = serverIps.split(DELIMITER_IP);
			for(int i = 0; i < ipList.length; i ++) 
			{
				ips.put(i + 1, ipList[i].trim());
			}
		}
	}
}
