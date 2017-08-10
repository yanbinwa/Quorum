package org.com.packagebase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.com.constant.QuorumConstant;
import org.com.data.Record;
import org.com.exception.DeSerizliationException;
import org.com.exception.SerizliationException;
import org.com.util.BinaryInputStream;
import org.com.util.BinaryOutputStream;

/*
 * 4. syncPackage的结构
 * 
 * SNYC_UP: tag + myId + myEpoch + myZxid
 * SNYC_BACK: tag + leaderId + leaderEpoch + leaderZxid + recordNum + record1 + record2 + ...
 * START_WORK: tag + leaderId + leaderEpoch + leaderZxid
 * WORK_REQUEST: tag + myId + myEpoch + myZxid + record
 * WORK_PROMOTE: tag + leaderId + leaderEpoch + leaderZxid + recordID + record
 * WORK_REPLY: tag + myId + myEpoch + myZxid + recordID + isOk
 * WORK_COMMIT: tag + leaderId + leaderEpoch + leaderZxid + recordID + isOk
 */

public class SyncPackage extends PackageBase {

	List<Record> records = null;
	int type = -1;
	int serverId = -1;
	int serverEpoch = -1;
	long serverZxid = -1;
	long recordID = -1;
	boolean isOk = false;
	
	public SyncPackage(int type, int serverId, int serverEpoch, 
			long serverZxid, List<Record> records, long recordID, boolean isOk)
	{
		this.records = records;
		this.type = type;
		this.serverId = serverId;
		this.serverEpoch = serverEpoch;
		this.serverZxid = serverZxid;
		this.recordID = recordID;
		this.isOk = isOk;
	}
	
	public SyncPackage()
	{
		
	}
	
	public int getType()
	{
		return type;
	}
	
	public int getServerId()
	{
		return serverId;
	}
	
	public int getServerEpoch()
	{
		return serverEpoch;
	}
	
	public long getServerZxid()
	{
		return serverZxid;
	}
	
	public List<Record> getRecords()
	{
		return records;
	}
	
	public Record getRecord()
	{
		if (records == null || records.isEmpty())
		{
			return null;
		}
		return records.get(0);
	}
	
	public long getRecordID()
	{
		return recordID;
	}
	
	public String toString()
	{
		String ret = String.format("type is: %d, serverId is %d, serverEpoch is %d, "
				 + "serverZxid is: %d, recordID is %d, isOk is %d ",
				 type, serverId, serverEpoch,
				 serverZxid, recordID, isOk);

		return ret;
	}
	
	@Override
	public byte[] serizlization() throws SerizliationException {
		// TODO Auto-generated method stub
		ByteArrayOutputStream baos = null;
		try 
		{
			/**SNYC_UP AND START_WORK*/
			baos = new ByteArrayOutputStream();
			BinaryOutputStream bos = new BinaryOutputStream(baos);
			bos.writeInt(type);
			bos.writeInt(serverId);
			bos.writeInt(serverEpoch);
			bos.writeLong(serverZxid);
			switch(type)
			{
			case QuorumConstant.SYNC_SNYC_BACK:
				bos.writeInt(records.size());
				for(Record record : records)
				{
					byte[] buf = record.serizlization();
					bos.writeInt(buf.length);
					bos.writeByte(buf);
				}
				break;
				
			case QuorumConstant.SYNC_WORK_REQUEST:
				Record record = records.get(0);
				byte[] buf = record.serizlization();
				bos.writeInt(buf.length);
				bos.writeByte(buf);
				break;
				
			case QuorumConstant.SYNC_WORK_PROMOTE:
				bos.writeLong(recordID);
				Record record1 = records.get(0);
				byte[] buf1 = record1.serizlization();
				bos.writeInt(buf1.length);
				bos.writeByte(buf1);
				break;
				
			case QuorumConstant.SYNC_WORK_REPLY:
			case QuorumConstant.SYNC_WORK_COMMIT:
				bos.writeLong(recordID);
				bos.writeBoolean(isOk);
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
			type = bis.readInt();
			serverId = bis.readInt();
			serverEpoch = bis.readInt();
			serverZxid = bis.readLong();
			records = new ArrayList<Record>();
			
			switch(type)
			{
			case QuorumConstant.SYNC_SNYC_BACK:
				int recordNum = bis.readInt();
				for(int i = 0; i < recordNum; i ++)
				{
					Record record = new Record();
					int recordLen = bis.readInt();
					byte[] buf = bis.readByte(recordLen);
					record.deSerizliation(buf);
					records.add(record);
				}
				break;
				
			case QuorumConstant.SYNC_WORK_REQUEST:
				Record record = new Record();
				int recordLen = bis.readInt();
				byte[] buf = bis.readByte(recordLen);
				record.deSerizliation(buf);
				records.add(record);
				break;
				
			case QuorumConstant.SYNC_WORK_PROMOTE:
				Record record1 = new Record();
				recordID = bis.readLong();
				int recordLen1 = bis.readInt();
				byte[] buf1 = bis.readByte(recordLen1);
				record1.deSerizliation(buf1);
				records.add(record1);
				break;
				
			case QuorumConstant.SYNC_WORK_REPLY:
			case QuorumConstant.SYNC_WORK_COMMIT:
				recordID = bis.readLong();
				isOk = bis.readBoolean();
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
