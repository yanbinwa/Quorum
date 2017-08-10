package org.com.packagebase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import org.com.constant.QuorumConstant;
import org.com.exception.DeSerizliationException;
import org.com.exception.SerizliationException;
import org.com.util.BinaryInputStream;
import org.com.util.BinaryOutputStream;

/**
 * SelectPackage的格式为：
 * type(int) + myid(int) + myEpoch(long) + myZxid(long) + 
 * serverId(long) + serverEpoch(long) + serverZxid(long) 
 * 
 * type指明该package是否需要send back
 * 
 * 对于发送选票的起始方，需要对方的应答，而如果是被发送方，则不需要对方的应答
 */

public class SelectPackage extends PackageBase {
	
	/**是否需要应答*/
	int type;
	
	/**发送对方的Id*/
	int targetServerId;
	
	/**发送方的当前的状态：Looking，Leader or Follower*/
	int myStatus;
	int myId;
	int myEpoch;
	long myZxid;
	
	int serverId;
	int serverEpoch;
	long serverZxid;
	
	public SelectPackage(int type, int targetServerId, int myStatus, int myId, int myEpoch, long myZxid, 
			int serverId, int serverEpoch, long serverZxid)
	{
		this.type = type;
		this.targetServerId = targetServerId;
		
		this.myStatus = myStatus;
		this.myId = myId;
		this.myEpoch = myEpoch;
		this.myZxid = myZxid;
		
		this.serverId = serverId;
		this.serverEpoch = serverEpoch;
		this.serverZxid = serverZxid;
	}
	
	public SelectPackage()
	{
		
	}
	
	public int getTargetServerId()
	{
		return targetServerId;
	}
	
	public int getVoterStatus()
	{
		return myStatus;
	}
	
	public int getVoterId()
	{
		return myId;
	}
	
	public int getVoterEpoch()
	{
		return myEpoch;
	}
	
	public long getVoterZxid()
	{
		return myZxid;
	}
	
	public int getSelectorId()
	{
		return serverId;
	}
	
	public int getSelectorEpoch()
	{
		return serverEpoch;
	}
	
	public long getSelectorZxid()
	{
		return serverZxid;
	}
	
	public boolean isNeedAck()
	{
		if (type == QuorumConstant.SELECT_PACKAGE_NEED_ACK)
			return true;
		else
			return false;
	}
	
	@Override
	public byte[] serizlization() throws SerizliationException {
		// TODO Auto-generated method stub
		try
		{
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			BinaryOutputStream bos = new BinaryOutputStream(baos);
			bos.writeInt(type);
			bos.writeInt(targetServerId);
			
			bos.writeInt(myStatus);
			bos.writeInt(myId);
			bos.writeInt(myEpoch);
			bos.writeLong(myZxid);
			
			bos.writeInt(serverId);
			bos.writeInt(serverEpoch);
			bos.writeLong(serverZxid);
			
			return baos.toByteArray();
		}
		catch(Exception e)
		{
			throw new SerizliationException();
		}
	}

	@Override
	public void deSerizliation(byte[] buffer) throws DeSerizliationException {
		// TODO Auto-generated method stub
		try
		{
			ByteArrayInputStream bios = new ByteArrayInputStream(buffer);
			BinaryInputStream bis = new BinaryInputStream(bios);
			type = bis.readInt();
			targetServerId = bis.readInt();
			
			myStatus = bis.readInt();
			myId = bis.readInt();
			myEpoch = bis.readInt();
			myZxid = bis.readLong();
			
			serverId = bis.readInt();
			serverEpoch = bis.readInt();
			serverZxid = bis.readLong();
		}
		catch(Exception e)
		{
			throw new DeSerizliationException();
		}
	}	
	
	public String toString()
	{
		String ret = String.format("type is: %d, targetServerId is %d, myStatus is %d, "
								 + "myId is: %d, myEpoch is %d, myZxid is %d "
								 + "serverId is: %d, serverEpoch is %d, serverZxid is %d ",
								 type, targetServerId, myStatus,
								 myId, myEpoch, myZxid,
								 serverId, serverEpoch, serverZxid);
		
		return ret;
	}
}
