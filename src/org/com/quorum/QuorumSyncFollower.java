package org.com.quorum;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.com.connection.Session;
import org.com.connection.SyncConnectionFollower;
import org.com.constant.QuorumConstant;
import org.com.data.Record;
import org.com.logger.QuorumLogger;
import org.com.packagebase.PackageBase;
import org.com.packagebase.SyncPackage;

/**
 * QuorumSync类实现了follower与leader之间同步的上层逻辑，这里包括三个部分
 * 
 * 1. 当looking变为follower后，会尝试向leader的sync port发起连接，如果失败（leader还没有建立），sleep一段
 * 时间后重连。一旦连接成功，就向leader发送自己的epoch和zxid，之后获取到diff信息，更新后返回ACK。
 * 
 * 2. 当looking变为leader后，等待follower的连接，接收到对方的epoch和zxid，返回diff信息，之后接受到follower
 * 的ACK信号。当返回的ACK数超过二分之一后，leader就发送开始正式工作的信号，这时leader和follower就可以接收client
 * 端的操作了
 * 
 * 3. follower与leader之间的同步标签有：
 * 
 * SNYC_UP: follower向leader发送的请求更新数据的信息（一旦发现自己的zxid与leaderzxid不一致，就要发送sync up的请求）
 * SNYC_BACK:  leader向follower发送的应答信息
 * SNYC_UP_COMMIT: follower完成sync up后对leader的应答
 * 
 * START_WORK:	leader向follower发送的开始工作的信息
 * START_WORK_COMMIT: follower完成sync up后对leader的应答
 * 
 * WORK_REQUEST: follower向leader发起数据修改请求（增，删，改）
 * WORK_PROMOTE: leader将follower的建议发给所有的follower来进行选举
 * WORK_REPLY:	follower向leader表示可以执行该任务，但还没有执行
 * WORK_COMMIT: 当leader获取半数以上的支持时，先执行该修改，之后发送commit信息，follower接受到commit后开始执行
 * WORK_REJECT: 当相应的follower没有超过半数，leader将会驳回follower的请求，最初发起work的follower会向client发送
 * reject信息
 * 
 * 对于每一个work，leader都要有一个递增的work id来唯一标识，
 * 
 * 4. syncPackage的结构
 * 
 * SNYC_UP: tag + myId + myEpoch + myZxid
 * SNYC_BACK: tag + leaderId + leaderEpoch + leaderZxid + recordNum + record1 + record2 + ...
 * SYYC_UP_COMMIT: tag + myId + myEpoch + myZxid
 * 
 * START_WORK: tag + leaderId + leaderEpoch + leaderZxid
 * 
 * 
 * WORK_REQUEST: tag + myId + myEpoch + myZxid + record
 * WORK_PROMOTE: tag + leaderId + leaderEpoch + leaderZxid + recordID + record
 * WORK_REPLY: tag + myId + myEpoch + myZxid + recordID + isOk
 * WORK_COMMIT: tag + leaderId + leaderEpoch + leaderZxid + recordID
 * WORK_REJECT: tag + leaderId + leaderEpoch + leaderZxid + recordID
 * 
 * 5. record结构
 * 
 * 操作类型（ADD，UPDATE, DELETE）
 * 
 * ADD: tag + 路径长度 + 路径 + 变量类型 + 变量数值 (解析后变量类型，就可以调用相应的方法来获取变量值了)
 * UPDATE: tag + 路径长度 + 路径 + 变量类型 + 变量数值
 * DELETE: tag + 路径长度 + 路径
 * 
 * 6. 使用的Queue
 * 
 * 7. Follower 工作有三个状态：
 * a. leader的sync up过程，只接受SYNC_BACK
 * b. 等待leader的start_work方法，只接受START_WORK package
 * c. 正式工作的方法
 */

public class QuorumSyncFollower extends QuorumSync {

	Quorum qm = null;
	SyncConnectionFollower scf = null;
	Session session = null;
	boolean finished = false;
	QuorumLogger qml = null;
	int followerStatus = QuorumConstant.FOLLOWER_WAIT_TO_SYNC;
	Map<Long, Record> waitToCommitRecord = null;
	
	LinkedBlockingQueue<PackageBase> syncSendBufferQueue = null;
	
	public QuorumSyncFollower(Quorum quorum)
	{
		qm = quorum;
		scf = new SyncConnectionFollower(qm, this, qm.getSyncPort());
		syncSendBufferQueue = new LinkedBlockingQueue<PackageBase>();
		waitToCommitRecord = new HashMap<Long, Record>();
		qml = QuorumLogger.getInstance();
	}
	
	private void sendPackage(PackageBase pk)
	{
		boolean ret = scf.sendPackage(pk);
		if (!ret)
		{
			try
			{
				syncSendBufferQueue.put(pk);
			}
			catch(InterruptedException e)
			{
				e.printStackTrace();
			}
		}
	}
	
	private void sendPackageFromQueue()
	{
		Thread thread = new Thread(
				new Runnable() 
				{

					@Override
					public void run() {
						// TODO Auto-generated method stub
						// 这里需要sleep
						try
						{
							PackageBase pk = null;
							while(!finished)
							{
								/*try to run */
								if (pk == null)
								{
									pk = syncSendBufferQueue.take();
								}
								boolean ret = scf.sendPackage(pk);
								if (ret)
								{
									pk = null;
								}
								else
								{
									Thread.sleep(1000);
								}
							}
						}
						catch(Exception e)
						{
							qml.logger.severe("step7" + e.getMessage());
						}
					}
					
				});
		thread.start();
	}
	
	public void startUp()
	{
		scf.startUp();
		sendPackageFromQueue();
	}
	
	public void syncConnectReady()
	{
		session = new Session(scf, QuorumConstant.SYNC_INTERVAL);
		session.startUp();
		
	}

	private PackageBase generateSyncUpRequest()
	{
		SyncPackage spk = new SyncPackage(QuorumConstant.SYNC_SNYC_UP, qm.getMyId(), qm.getMyEpoch(), 
				qm.getMyZxid(), null, -1, false);
		return spk;
	}
	
	private PackageBase generateSyncUpCommit()
	{
		SyncPackage spk = new SyncPackage(QuorumConstant.SYNC_SNYC_UP_COMMIT, qm.getMyId(), qm.getMyEpoch(), 
				qm.getMyZxid(), null, -1, false);
		return spk;
	}
	
	private PackageBase generateStartWorkCommit()
	{
		SyncPackage spk = new SyncPackage(QuorumConstant.SYNC_START_WORK_COMMIT, qm.getMyId(), qm.getMyEpoch(), 
				qm.getMyZxid(), null, -1, false);
		return spk;
	}
	
	private PackageBase generateWorkReply(long recordID)
	{
		SyncPackage spk = new SyncPackage(QuorumConstant.SYNC_WORK_REPLY, qm.getMyId(), qm.getMyEpoch(), 
				qm.getMyZxid(), null, recordID, false);
		return spk;
	}
	
	private boolean checkNeedToSyncUp(SyncPackage spk)
	{
		if (qm.getMyZxid() < spk.getServerZxid())
		{
			return true;
		}
		return false;
	}
	
	private void startToSyncUp()
	{
		PackageBase request = generateSyncUpRequest();
		sendPackage(request);
	}
	
	private void handleWaitToSync(SyncPackage spk)
	{
		/**读取record list来修改DataTree*/
		if (spk.getType() != QuorumConstant.SYNC_SNYC_BACK)
		{
			qml.logger.severe("handleWaitToSync get wrong package. The package is " + spk);
			return;
		}
		List<Record> records = spk.getRecords();
		if (records == null)
		{
			qml.logger.severe("records is null. The package is " + spk);
			return;
		}
		if (records.isEmpty())
		{
			qml.logger.info("do not need to update");
			return;
		}
		qm.updateDatabase(records);
		/**更新自己的纪元*/
		qm.setMyEpoch(spk.getServerEpoch());
		qm.setMyZxid(spk.getServerZxid());
		qm.updateMyQuorumInfo();
		/**返回commit信息*/
		PackageBase syncCommit = generateSyncUpCommit();
		this.sendPackage(syncCommit);
		followerStatus = QuorumConstant.FOLLOWER_WAIT_TO_START;
	}
	
	private void handleWaitToStart(SyncPackage spk)
	{
		if (spk.getType() != QuorumConstant.SYNC_START_WORK)
		{
			qml.logger.severe("handleWaitToStart get wrong package. The package is " + spk);
			return;
		}
		boolean ret = checkNeedToSyncUp(spk);
		if (ret)
		{
			startToSyncUp();
			return;
		}
		PackageBase startWorkCommit = this.generateStartWorkCommit();
		this.sendPackage(startWorkCommit);
		followerStatus = QuorumConstant.FOLLOWER_WORKING;
	}
	
	private void handleWorking(SyncPackage spk)
	{
		if (spk.getType() == QuorumConstant.SYNC_SNYC_BACK ||
				spk.getType() == QuorumConstant.SYNC_START_WORK)
				
		{
			qml.logger.severe("handleWorking get wrong package. The package is " + spk);
			return;
		}
		boolean ret = checkNeedToSyncUp(spk);
		if (ret)
		{
			startToSyncUp();
			return;
		}
		int packageType = spk.getType();
		switch(packageType)
		{
		case QuorumConstant.SYNC_WORK_PROMOTE:
			/**
			 * 将recordID和record记录下来(Map)，等待之后的commit或者reject操作
			 */
			long recordID = spk.getRecordID();
			if (recordID == -1)
			{
				qml.logger.severe("handleWorking get wrong recordID. The package is " + spk);
				return;
			}
			Record record = spk.getRecord();
			if (record == null)
			{
				qml.logger.severe("handleWorking get empty record. The package is " + spk);
				return;
			}
			this.waitToCommitRecord.put(recordID, record);
			PackageBase workReply = this.generateWorkReply(recordID);
			sendPackage(workReply);
			break;
		case QuorumConstant.SYNC_WORK_COMMIT:
			/**
			 * 从Map中读取等待操作的record，update database，判断该修改请求是否是自己发出的，如果是自己发起的，向client返回结果。
			 */
			long execRecordID = spk.getRecordID();
			Record execRecord = waitToCommitRecord.get(execRecordID);
			qm.updateDatabase(execRecord);
			qm.increaseMyZxid();
			qml.logger.info("exec record " + execRecord);
			break;
		case QuorumConstant.SYNC_WORK_REJECT:
			/**
			 * 从Map中删除record，判断该修改请求是否是自己发出的，如果是自己发起的，向client返回结果。不需要给leader commit.
			 */
			Record abordRecord = waitToCommitRecord.remove(spk.getRecordID());
			qml.logger.info("abord record " + abordRecord);
			break;
		}
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		/**It is request quorum can start sync up*/
		PackageBase request = generateSyncUpRequest();
		sendPackage(request);
		
		while(!finished)
		{
			PackageBase pk = scf.getPackage();
			if (pk == null)
			{
				qml.logger.severe("Get null Package in QuorumSyncFollower");
				return;
			}
			if (!(pk instanceof SyncPackage))
			{
				continue;
			}
			SyncPackage spk = (SyncPackage) pk;
			qm.updateQuorumGroup(spk);
			switch(followerStatus)
			{
			case QuorumConstant.FOLLOWER_WAIT_TO_SYNC:
				handleWaitToSync(spk);
				break;
			case QuorumConstant.FOLLOWER_WAIT_TO_START:
				handleWaitToStart(spk);
				break;
			case QuorumConstant.FOLLOWER_WORKING:
				handleWorking(spk);
				break;
			}
		}
	}	
}
