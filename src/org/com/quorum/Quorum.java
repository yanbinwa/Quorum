package org.com.quorum;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.com.constant.QuorumConstant;
import org.com.data.DataTree;
import org.com.data.Record;
import org.com.data.Vote;
import org.com.logger.QuorumLogger;
import org.com.packagebase.PackageBase;

/**
 * Quorum主要维护数据的一致性（内存中的DataTree），其主要分为三个部分：1.Leader的选举； 
 * 2.数据的同步；3.客户端的交互(session的维护)。
 * 角色分为Looking，Leader和follower，
 *
 * 1. 选举
 * 所有server均为Looking状态，将其epoch和zxid广播给所有的server，依照serverid建立通信，
 * 当接受到其它server的选票后，与自己的判断，如果比自己好，就更新自己的选票，并通知给其它人，
 * 如果没有自己好，就把自己的选票回发给其它server。所以这个的package有两个type，第一的主动发
 * 送的type，需要对方来应答，第二个是应答type，对方不需要再返回。同时对自己维持的选票监控，如果
 * 满足leader条件，大于1/2的票投给自己，则该server变为leader，并将广播给follower。接受到
 * leader的通知的looking自动变为follower，开始与leader同步
 * 
 * 选票package的格式：myid，myepoch，myzxid，serverid，serverepoch，serverzxid，package type，myStatus（Looking。。。）
 * 
 * 通过package parse来获取相应的值
 * 
 * 
 * 2. 数据同步
 * follower or leader 接受到client的连接，发送修改数据的请求，该请求需要转给leader(即使leader
 * 自己接受到的client，也要传给自己)，之后leader将其发送给follower，一旦超过半数的follower（也
 * 包括leader）有应答后，leader会发送commit来使follower执行该命令，这里也发给自己，在server接收
 * 到commit命令时，修改自己的DataTree，并且比对该package是否是由其申请的，如果是自己申请的，要返回
 * client成功的信号
 * 
 * 请求package的格式：packagetype，myid，myepoch，myzxid，请求对象（record）
 * 
 * 返回package的格式：packagetype, leaderid，leaderepoch，leaderzxid（每一个修改动作都会导致zxid加1），record
 * 
 * 3. 客户端会向serveer端发起连接，server端通过select来接受请求，并建立session，客户端会定期发送
 * 心跳（PING）包，server接受到会直接应答，其余发送的是request包，该request最终由server转给leader
 * 统一处理
 * 
 * */

public class Quorum extends Thread
{

	int id;
	
	/**为原子类型*/	
	AtomicInteger epoch = new AtomicInteger();
	
	/**zxid的前32位为epoch*/
	AtomicLong zxid = new AtomicLong();
	
	/**这里的变量也都应该是原子操作*/
	Integer serverType;
	Boolean finished = false;

	QuorumGroupInfo quorumGroupInfo = null;
	QuorumConfig quorumConfig = null;
	QuorumSelection quorumSelection = null;
	QuorumSync quorumSync = null;
	
	QuorumLogger qml = null;
	DataTree database = null;
	
	public Quorum() 
	{
		this.init();
	}
	
	private void init() 
	{
		quorumConfig = new QuorumConfig();
		quorumConfig.loadConfig();
		quorumGroupInfo = new QuorumGroupInfo(quorumConfig);
		id = quorumConfig.myId;
		epoch.set(0);
		zxid.set(0L);
		serverType = QuorumConstant.SERVER_TYPE_LOOKING;
		qml = QuorumLogger.getInstance();
	}
	
	public void startUp()
	{
		startUpConnection();
		startUpSelection();
	}
	
	/**
	 * 创建与client之间的连接，包括一个accept和若干个select线程，一旦有request请求，就讲获得的socket
	 * 注册到相应的select线程中，由其来维护，所以accept线程是持有所有select线程的。
	 */
	private void startUpConnection()
	{
		
	}
	
	/**
	 * 创建accept，如果有连接请求，读取其serverId，判断是否建立连接
	 */
	private void startUpSelection() 
	{
		quorumSelection = new QuorumSelection(this);
		quorumSelection.startUp();
	}
	
	private void startUpSyncLeader()
	{
		quorumSync = new QuorumSyncLeader(this);
		quorumSync.startUp();
	}
	
	private void startUpSyncFollower()
	{
		quorumSync = new QuorumSyncFollower(this);
		quorumSync.startUp();
	}
	
	public String getServerIp(int id)
	{
		return quorumConfig.ips.get(id);
	}
	
	public int getMyId()
	{
		return id;
	}
	
	public int getMyEpoch()
	{
		return epoch.get();
	}
	
	public void setMyEpoch(int epoch)
	{
		this.epoch.set(epoch);
	}
	
	public void increaseMyEpoch()
	{
		this.epoch.incrementAndGet();
	}
	
	public long getMyZxid()
	{
		return zxid.get();
	}
	
	public void setMyZxid(long zxid)
	{
		this.zxid.set(zxid);
	}
	
	public void increaseMyZxid()
	{
		this.zxid.incrementAndGet();
	}
	
	public int getSelectPort()
	{
		return quorumConfig.selectPort;
	}
	
	public int getSyncPort()
	{
		return quorumConfig.syncPort;
	}
	
	public int getClientPort()
	{
		return quorumConfig.clientPort;
	}
	
	public int getMyServerType()
	{
		synchronized(serverType)
		{
			return serverType;
		}
	}
	
	public void setMyServerType(int type)
	{
		synchronized(serverType)
		{
			serverType = type;
		}
	}
	
	public int getEpochById(int id)
	{
		return quorumGroupInfo.getEpochById(id);
	}
	
	public void setEpochById(int id, int epoch)
	{
		quorumGroupInfo.setEpochById(id, epoch);
	}
	
	public long getZxidById(int id)
	{
		return quorumGroupInfo.getZxidById(id);
	}
	
	public void setZxidById(int id, long zxid)
	{
		quorumGroupInfo.setZxidById(id, zxid);
	}
	
	public Set<Integer> getServerIds()
	{
		return quorumGroupInfo.getServerIds();
	}
	
	public int voteLeader(Map<Integer, Vote> votes)
	{
		return quorumGroupInfo.voteLeader(votes);
	}
	
	public void updateQuorumGroup(PackageBase pk)
	{
		quorumGroupInfo.updateQuorumGroup(pk);
	}
	
	public void updateMyQuorumInfo()
	{
		quorumGroupInfo.setEpochById(this.getMyId(), this.getMyEpoch());
		quorumGroupInfo.setZxidById(this.getMyId(), this.getMyZxid());
	}
	
	public void setLeaderId(int id)
	{
		quorumGroupInfo.setLeaderId(id);
	}
	
	public int getLeaderId()
	{
		return quorumGroupInfo.getLeaderId();
	}
	
	public void updateDatabase(List<Record> records)
	{
		this.database.updateDatabase(records);
	}
	
	public void updateDatabase(Record record)
	{
		this.database.updateDatabase(record);
	}
	
	
	/**
	 * 根据server当前的状态的Looking，Leader and Follower来执行不同的操作
	 */
	@Override
	public void run() {
		// TODO Auto-generated method stub
		try
		{
			qml.logger.info("Quorum is start");
			while(!finished)
			{
				switch(serverType)
				{
				
				/**
				 * 开始selection逻辑，接受SelectPackage
				 */
				case QuorumConstant.SERVER_TYPE_LOOKING:
					qml.logger.info("SERVER_TYPE_LOOKING");
					while(!finished)
					{
						if (serverType != QuorumConstant.SERVER_TYPE_LOOKING)
						{
							break;
						}
						Thread.sleep(1000);
					}
					break;
				
				/**
				 * 保持与Leader的sync，接受Leader的SyncPackage
				 */
				case QuorumConstant.SERVER_TYPE_FOLLOWER:
					qml.logger.info("SERVER_TYPE_FOLLOWER");
					/**
					 * follower第一步就是将自己的Epoch和zxid发送给leader，这里会返回leader当前的epoch和zxid以及与
					 * follower的zxid对比后得到的差异这里将quorun中每一个操作都记录为一个recode，这样只要比较差异，
					 * 返回之前的recode，follower就可以通过recode来恢复数据。当数据恢复后才可以响应client的操作
					 */
				
//					startUpSyncFollower();
					while(!finished)
					{
						qml.logger.info("Follower");
						if (serverType != QuorumConstant.SERVER_TYPE_FOLLOWER)
						{
							break;
						}
						Thread.sleep(1000);
					}
					break;
				
				/**
				 * Leader all request
				 */
				case QuorumConstant.SERVER_TYPE_LEADER:
					System.out.println("SERVER_TYPE_LEADER");
					/**
					 * leader可以接受follwer sync的请求，并返回更新后的状态，也可以接受follower的request
					 */

//					startUpSyncLeader();
					while(!finished)
					{
						qml.logger.info("Leader");
						if (serverType != QuorumConstant.SERVER_TYPE_LEADER)
						{
							break;
						}
						Thread.sleep(1000);
					}
					break;
				}
			}
		}
		catch (InterruptedException e) 
		{
			qml.logger.severe("step6" + e.getMessage());
		}
	}
	
	public static void main(String[] args) 
	{
		Quorum quorum = new Quorum();
		quorum.start();
		quorum.startUp();		
	}
}
