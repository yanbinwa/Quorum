package org.com.connection;

import java.util.HashMap;
import java.util.Map;

import org.com.constant.QuorumConstant;

/**
 * 1. 生成sessionID
 * 2. 
 */

public class SessionManager 
{
	Map<Long, SessionInfo> sessionInfos = null;
	Connection connection = null;
	boolean finished = false;
	
	public SessionManager(Connection connection)
	{
		sessionInfos = new HashMap<Long, SessionInfo>();
		this.connection = connection;
	}
	
	public long generateSessionID(int seed)
	{
		return seed;
	}
	
	public void sessionUpdate(long sessionID, int interval, int serverId)
	{
		synchronized(sessionInfos)
		{
			SessionInfo sessionInfo = sessionInfos.get(sessionID);
			if (sessionInfo == null)
			{
				sessionInfo = new SessionInfo();
				if (sessionID == Long.MIN_VALUE)
				{
					/**需要生成新的sessionID*/
					sessionID = generateSessionID(serverId);
					sessionInfo.sessionID = sessionID;
				}
			}
			if (sessionInfo.sessionID != sessionID)
			{
				/**几乎不可能出现，如果出现，会断开现在的连接，删除该session*/
				connection.sessionTimeout(sessionID);
				sessionInfos.remove(sessionID);
				return;
			}
			sessionInfo.sessionLastReceive = System.currentTimeMillis();
			sessionInfo.interval = interval;
		}
	}
	
	public void startUp()
	{
		/**通过线程来查询是否有sessionTimeout*/
		Thread thread = new Thread(
				new Runnable()
				{

					@Override
					public void run() {
						// TODO Auto-generated method stub
						while(!finished)
						{
							checkSession();
							try 
							{
								Thread.sleep(1000);
							} 
							catch (InterruptedException e) 
							{
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
					}
					
				});
		
		thread.start();
	}
	
	public void checkSession()
	{
		long currentTimestamp = System.currentTimeMillis();
		synchronized(sessionInfos)
		{
			for (Map.Entry<Long, SessionInfo> entry : sessionInfos.entrySet())
			{
				SessionInfo sessionInfo = entry.getValue();
				if (currentTimestamp - QuorumConstant.SESSION_DELAY > 
						sessionInfo.sessionLastReceive + sessionInfo.interval * 1000)
				{
					connection.sessionTimeout(sessionInfo.sessionID);
					sessionInfos.remove(sessionInfo.sessionID);
				}
			}
		}
	}
	
	class SessionInfo
	{
		public long sessionLastReceive;
		public int interval;
		public long sessionID;
	}
}
