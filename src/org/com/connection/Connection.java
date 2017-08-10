package org.com.connection;

import java.io.IOException;

public abstract class Connection 
{
	public abstract void sendHeatBeat(long sessionId) throws IOException;
	public abstract void sessionTimeout();
	public abstract void sessionTimeout(long sessionID);
}
