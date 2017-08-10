package org.com.data;

import org.com.packagebase.PackageBase;
import org.com.packagebase.SelectPackage;

/**
 * 选举所用的vote
 *
 */

public class Vote {
	
	public int voterId;
	public int voterEpoch;
	public long voterZxid;
	
	public int selectId;
	public int selectEpoch;
	public long selectZxid;
	
	public Vote(int voterId, int voterEpoch, long voterZxid,
			int selectId, int selectEpoch, long selectZxid)
	{
		this.voterId = voterId;
		this.voterEpoch = voterEpoch;
		this.voterZxid = voterZxid;
		this.selectId = selectId;
		this.selectEpoch = selectEpoch;
		this.selectZxid = selectZxid;
	}
	
	public Vote(PackageBase pk)
	{
		if(pk instanceof SelectPackage)
		{
			SelectPackage spk = (SelectPackage)pk;
			this.voterId = spk.getVoterId();
			this.voterEpoch = spk.getVoterEpoch();
			this.voterZxid = spk.getVoterZxid();
			this.selectId = spk.getSelectorId();
			this.selectEpoch = spk.getSelectorEpoch();
			this.selectZxid = spk.getSelectorZxid();
		}
	}	
	
	public boolean challengeVote(Vote vote)
	{
		if (this.selectZxid > vote.selectEpoch)
		{
			return true;
		}
		else if(this.selectZxid < vote.selectEpoch)
		{
			return false;
		}
		
		if (this.selectId >= vote.selectId)
		{
			return true;
		}
		else 
		{
			return false;
		}
	}
	
	public void updateVote(Vote vote)
	{
		this.selectId = vote.selectId;
		this.selectZxid = vote.selectZxid;
	}
	
	public String toString()
	{
		String ret = String.format("voterId is: %d, voterEpoch is %d, voterZxid is %d, "
								 + "selectId is: %d, selectEpoch is %d, selectZxid is %d", 
								 voterId, voterEpoch, voterZxid,
								 selectId, selectEpoch, selectZxid);
		
		return ret;
	}
}
