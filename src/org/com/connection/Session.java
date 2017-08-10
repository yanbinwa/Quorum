package org.com.connection;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.com.constant.QuorumConstant;

/**
 * 对于Session管理是有需求的是一下两种场景
 * 1. follower和leader对于client连接的session管理
 * 2. 在syncConnection中leader管理来自follower的连接
 * 
 * session的维持是这样的，client或者follower发出ping package，leader接收到ping包后更新session失效
 * 的时间，leader端会不断的检查是不是有session timeout了，如果timeout，就认定该follower失联了，就会断开
 * 与该client或者follower的连接，同时更新该follower为失联状态（QuorumGroupInfo中保存）。
 * 
 * follower端与client端的操作：
 * 在follower端设置一个定时任务来发送ping包给leader，并且设置下一次发送的时间。follower端一旦发现与leader的
 * connect中断后，先发送一个ping包给leader，看leader是否存活，并且状态仍然是leader，如果存在且为leader，
 * 再次发起SYNC_UP等等的操作，如果不存在，退出follower状态，再次发动选举。（这里的epoch是不会改变的）
 * 
 * Session -> SessionManager
 * 
 * PING package：
 * tag + serverId + serverEpoch + serverZxid + 
 * 
 */

public class Session 
{
	int interval;
	Long lastReceiveTimestamp = Long.MIN_VALUE;
	Long lastSendTimestamp = Long.MIN_VALUE;
	Connection connection = null;
	long sessionID = Long.MIN_VALUE;
	ScheduledExecutorService service = null;
	
	public Session(Connection connection, int interval)
	{
		this.connection = connection;
		this.interval = interval;
	}
	
	public void startUp()
	{
		Runnable runnable = new Runnable()
		{
			@Override
			public void run() {
				// TODO Auto-generated method stub
				try 
				{
					synchronized(lastReceiveTimestamp)
					{
						if (lastReceiveTimestamp != Long.MIN_VALUE)
						{
							if (System.currentTimeMillis() - QuorumConstant.SESSION_DELAY
									> lastReceiveTimestamp + interval * 1000)
							{
								/**说明session 超时了*/
								connection.sessionTimeout();
								service.shutdownNow();
								return;
							}
						}
					}
					
					connection.sendHeatBeat(sessionID);
					synchronized(lastSendTimestamp)
					{
						lastSendTimestamp = System.currentTimeMillis();
					}
					/**这里还要检查leader返回的应答信息，如果上一次超时，这时也要处理*/
				} 
				catch (IOException e)
				{
					// TODO Auto-generated catch block
					service.shutdownNow();
					e.printStackTrace();
				}
			}
		};
		service = Executors.newSingleThreadScheduledExecutor();
		service.scheduleAtFixedRate(runnable, interval, interval, TimeUnit.SECONDS);
	}
	
	public void shutdown()
	{
		service.shutdown();
	}
	
	public void sessionUpdate(long sessionID)
	{
		if (this.sessionID != Long.MIN_VALUE)
		{
			this.sessionID = sessionID;
		}
		synchronized(lastReceiveTimestamp)
		{
			lastReceiveTimestamp = System.currentTimeMillis();
		}
	}
}
