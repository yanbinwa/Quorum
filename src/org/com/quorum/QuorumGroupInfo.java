package org.com.quorum;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.com.constant.QuorumConstant;
import org.com.data.Vote;
import org.com.packagebase.PackageBase;
import org.com.packagebase.SelectPackage;
import org.com.packagebase.SyncPackage;

/**
 * 存放Quorum集群信息，包括各个server的ip，也可以存放每个server的选票信息，状态信息（epoch，zxid，
 * looking or leader or follower）
 */

public class QuorumGroupInfo 
{
	
	Map<Integer, QuorumInfo> quorumInfos = null;
	int leaderId = -1;
	
	public QuorumGroupInfo(QuorumConfig quorumConfig) 
	{
		quorumInfos = new HashMap<Integer, QuorumInfo>();
		Map<Integer, String> ips = quorumConfig.ips;
		for(Map.Entry<Integer, String> entry : ips.entrySet())
		{
			QuorumInfo quorumInfo = new QuorumInfo(entry.getKey(), entry.getValue());
			quorumInfos.put(entry.getKey(), quorumInfo);
		}
	}
	
	/**
	 * 首先将将vote投给相应的quorum，选择选票最多的，看其选票是否超过1/2
	 * 
	 * 如果超过1/2，则判断该server是否也投了自己，如果投了自己，返回serverId，否则返回-1
	 */
	public int voteLeader(Map<Integer, Vote> votes)
	{
		if (votes == null)
		{
			return -1;
		}
		
		clearVoteNumber();
		for(Vote vote : votes.values())
		{
			int selectId = vote.selectId;
			if (quorumInfos.containsKey(selectId))
			{
				quorumInfos.get(selectId).voteNumber ++;
			}
		}
		
		int selectResultId = -1;
		for(QuorumInfo quorumInfo : quorumInfos.values())
		{
			System.out.println("quorumInfo.voteNumber is: " + quorumInfo.voteNumber);
			if(quorumInfo.voteNumber > getQuorumSize() / 2)
			{
				selectResultId = quorumInfo.id;
			}
		}
		
		if (selectResultId == -1)
		{
			return -1;
		}
		
		if (!votes.containsKey(selectResultId))
		{
			return -1;
		}
		
		/**选出的leader也选择了自己*/
		if (votes.get(selectResultId).selectId == selectResultId)
		{
			System.out.println("this final result is: " + selectResultId);
			return selectResultId;
		}
		else
		{
			
			return -1;
		}
	}
	
	public Set<Integer> getServerIds()
	{
		return quorumInfos.keySet();
	}
	
	private void clearVoteNumber()
	{
		for(QuorumInfo quorumInfo : quorumInfos.values())
		{
			quorumInfo.voteNumber = 0;
		}
	}
	
	private int getQuorumSize()
	{
		return quorumInfos.size();
	}
	
	public void updateQuorumGroup(PackageBase pk)
	{
		if (pk instanceof SelectPackage)
		{
			SelectPackage spk = (SelectPackage) pk;
			int serverId = spk.getVoterId();
			QuorumInfo qif = quorumInfos.get(serverId);
			if (qif == null)
			{
				qif = new QuorumInfo(serverId, null);
			}
			qif.epoch = spk.getVoterEpoch();
			qif.zxid = spk.getVoterZxid();
			qif.serverType = spk.getVoterStatus();
		}
		else if(pk instanceof SyncPackage)
		{
			SyncPackage spk = (SyncPackage) pk;
			int serverId = spk.getServerId();
			QuorumInfo qif = quorumInfos.get(serverId);
			if (qif == null)
			{
				qif = new QuorumInfo(serverId, null);
			}
			qif.epoch = spk.getServerEpoch();
			qif.zxid = spk.getServerZxid();
		}
	}
	
	public int getEpochById(int id)
	{
		QuorumInfo qif = quorumInfos.get(id);
		if (qif == null)
		{
			return -1;
		}
		return qif.epoch;
	}
	
	public void setEpochById(int id, int epoch)
	{
		QuorumInfo qif = quorumInfos.get(id);
		if (qif != null)
		{
			qif.epoch = epoch;
		}
	}
	
	public long getZxidById(int id)
	{
		QuorumInfo qif = quorumInfos.get(id);
		if (qif == null)
		{
			return -1L;
		}
		return qif.zxid;
	}
	
	public void setZxidById(int id, long zxid)
	{
		QuorumInfo qif = quorumInfos.get(id);
		if (qif != null)
		{
			qif.zxid = zxid;
		}
	}
	
	public void setLeaderId(int id)
	{
		this.leaderId = id;
	}
	
	public int getLeaderId()
	{
		return this.leaderId;
	}
	
	public String getServerIp(int id)
	{
		QuorumInfo quorumInfo =  quorumInfos.get(id);
		if (quorumInfo == null)
		{
			return null;
		}
		return quorumInfo.ip;
	}
	
	class QuorumInfo 
	{
		public int id;
		public int epoch;
		public long zxid;
		public String ip;
		
		public int voteNumber;
		
		/**Looking leader follower*/
		public int serverType;
		
		public QuorumInfo(int id, String ip)
		{
			this.ip = ip;
			this.id = id;
			this.epoch = 0;
			this.zxid = 0L;
			this.serverType = QuorumConstant.SERVER_TYPE_LOOKING;
		}
		
	}
}
