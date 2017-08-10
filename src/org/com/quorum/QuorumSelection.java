package org.com.quorum;

import java.util.HashMap;
import java.util.Map;

import org.com.connection.SelectConnection;
import org.com.constant.QuorumConstant;
import org.com.data.Vote;
import org.com.logger.QuorumLogger;
import org.com.packagebase.PackageBase;
import org.com.packagebase.SelectPackage;
import org.com.util.ZxidUtils;

/**
 * 持有SelectConnection，通过向queue中发送和读取package来实现选举的逻辑。
 * 
 * 这里还有两个线程，
 * 
 * 一个线程用于将selectionSendQueue中的package再次写入到selectConnection中
 * 一个线程用于判断当前是否完成选举
 */

public class QuorumSelection implements Runnable
{
	
	SelectConnection selectConnection = null;
	
	Quorum qm = null;
	
	Vote currentVote = null;
	
	boolean finished = false;
	
	Map<Integer, PackageBase> selectionSendBuffer = null;
	Map<Integer, Vote> selectVotes = null;
	
	QuorumLogger qml = null;
	
	public QuorumSelection(Quorum quroum)
	{
		qm = quroum;
		selectConnection = new SelectConnection(quroum, quroum.getSelectPort());
		selectionSendBuffer = new HashMap<Integer, PackageBase>();
		selectVotes = new HashMap<Integer, Vote>();
		qml = QuorumLogger.getInstance();
	}
	
	public void startUp()
	{
		selectConnection.startUp();
		startSelection();
		sendPackageFromQueue();
		this.run();
	}
	
	private PackageBase generateSelectPackage(int id, int tag)
	{
		return new SelectPackage(tag, id, qm.getMyServerType(), qm.getMyId(), 
				qm.getMyEpoch(), qm.getMyZxid(), currentVote.selectId, currentVote.selectEpoch, currentVote.selectZxid);
	}
	
	/**通过serverId在QuorumGroup中更新epoch和zxid
	 */
	private void createCurrentVote()
	{
		currentVote = new Vote(qm.getMyId(), qm.getMyEpoch(), qm.getMyZxid(), qm.getMyId(), qm.getMyEpoch(), qm.getMyZxid()); 
	}
	
	private void updateCurrentVote()
	{
		currentVote.voterEpoch = qm.getMyEpoch();
		currentVote.voterZxid = qm.getMyZxid();
		currentVote.selectEpoch = qm.getEpochById(currentVote.selectId);
		currentVote.selectZxid = qm.getZxidById(currentVote.selectId);
	}
	
	private void sendPackage(int id, PackageBase pk) throws InterruptedException
	{
		boolean ret = selectConnection.sendPackage(id, pk);
		if (!ret)
		{
			synchronized(selectionSendBuffer)
			{
				selectionSendBuffer.put(id, pk);
			}
		}
	}
	
	private void startSelection()
	{
		createCurrentVote();
		selectVotes.put(currentVote.voterId, currentVote);
		Thread thread = new Thread(
				new Runnable() 
				{

					@Override
					public void run() {
						// TODO Auto-generated method stub
						selectLeader();
					}
					
				});
		thread.start();
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
							while(!finished)
							{
								Map<Integer, PackageBase> tmpBuffer = null;
								synchronized(selectionSendBuffer)
								{
									tmpBuffer = new HashMap<Integer, PackageBase>(selectionSendBuffer);
									selectionSendBuffer.clear();
								}
								for(Map.Entry<Integer, PackageBase>entry : tmpBuffer.entrySet())
								{
									/**这里要将旧的notify改为新的*/
									qml.logger.info("sendPackageFromQueue the package is: " + entry.getValue());
									sendPackage(entry.getKey(), entry.getValue());
								}
								Thread.sleep(100);
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
	
	private void updateSelectVotes(Vote vote)
	{
		synchronized(selectVotes)
		{
			selectVotes.put(vote.voterId, vote);
		}
	}
	
	private void removeSelectVotes(int id)
	{
		synchronized(selectVotes)
		{
			selectVotes.remove(id);
		}
	}
	
	private Vote getSelectVotes(int id)
	{
		synchronized(selectVotes)
		{
			return selectVotes.get(id);
		}
	}
	
	private void clearSelectVotes()
	{
		synchronized(selectVotes)
		{
			selectVotes.clear();
			selectVotes.put(qm.getMyId(), currentVote);
		}
	}
	
	private void finishedSelectLeader(int leaderId)
	{
		/**更改自己的状态，并且notify*/
		if (leaderId == qm.getMyId())
		{
			qm.setMyServerType(QuorumConstant.SERVER_TYPE_LEADER);
			qm.increaseMyEpoch();
			qm.setEpochById(qm.getMyId(), qm.getMyEpoch());
			qm.setMyZxid(ZxidUtils.makeZxid(qm.getMyEpoch(), 0));
			qm.setZxidById(qm.getMyId(), qm.getMyZxid());
		}
		else
		{
			qm.setMyServerType(QuorumConstant.SERVER_TYPE_FOLLOWER);
			qm.increaseMyEpoch();
			qm.setEpochById(qm.getMyId(), qm.getMyEpoch());
			/**follower the zxid是需要和leader sync之后才能产生的，这个逻辑是在其它地方实现的*/
		}
		qm.setLeaderId(leaderId);
		/**这里只是更新了vote中当前server的epoch和zxid*/
		updateCurrentVote();
		notifyAllServer(false);
		
		/**将selectVotes清空，即使当前选择的leader现在自己的状态不一定是leader*/
		clearSelectVotes();		
		/**这里并不进行vote中epoch的更新，server epoch仍然是旧的*/
	}
	
	/**作为一个线程来执行的*/
	private void selectLeader()
	{
		try
		{
			while(qm.getMyServerType() == QuorumConstant.SERVER_TYPE_LOOKING && !finished)
			{
				
				Map<Integer, Vote> sellectVoteTmp = null;
				synchronized(selectVotes)
				{
					sellectVoteTmp = new HashMap<Integer, Vote>(selectVotes);
				}			
				for (Vote vote : sellectVoteTmp.values())
				{
					qml.logger.info("selectLeader is: " + vote);
				}
				int selectId = qm.voteLeader(sellectVoteTmp);
				if (selectId > -1)
				{
					finishedSelectLeader(selectId);
					break;
				}
				Thread.sleep(10);
			}
		}
		catch (InterruptedException e) 
		{
			qml.logger.severe("step8" + e.getMessage());
		}
	}
	
	private void notifyAllServer(boolean isNeedAck)
	{
		for(int id : qm.getServerIds())
		{
			if (id == qm.getMyId())
			{
				continue;
			}
			qml.logger.info("notifyServer id is " + id);
			PackageBase pk = null;
			if (isNeedAck)
			{
				pk = generateSelectPackage(id, QuorumConstant.SELECT_PACKAGE_NEED_ACK);
			}
			else
			{
				pk = generateSelectPackage(id, QuorumConstant.SELECT_PACKAGE_NO_ACK);
			}
			try
			{
				sendPackage(id, pk);
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
		}
	}
	
	private boolean updateVote(Vote vote)
	{
		boolean ret = currentVote.challengeVote(vote);
		if (!ret)
		{
			currentVote.updateVote(vote);
		}
		else 
		{
			vote.updateVote(currentVote);
		}
		updateSelectVotes(vote);
		return !ret;
	}
	
	@Override
	public void run() {
		//消费selectConnection中的queue，并且向selectConnection写入package
		notifyAllServer(true);
		try
		{
			while(!finished)
			{
				PackageBase pk = selectConnection.getPackage();
				qml.logger.info("Receive the package: " + pk);
				if (!(pk instanceof SelectPackage))
				{
					continue;
				}
				
				SelectPackage spk = (SelectPackage)pk;
				qm.updateQuorumGroup(spk);
				Vote vote = new Vote(spk);
				int serverStatus = spk.getVoterStatus();
				int myStatus = qm.getMyServerType();
				
				int myEpoch = qm.getMyEpoch();
				int voteEpoch = spk.getVoterEpoch();
				
				/**
				 * 这里是有问题的，如果对方是follower or leader的话，会epoch ++ ，如果此时还是looking，肯定epoch要比对方低，
				 * 这里不能直接就进行如下的处理！！！还是要分情况讨论
				 * 
				 * 1. 如果当前为looking状态：
				 * 		a. 如果对方为looking状态：
				 * 			epoch > myepoch: 则我要更新自己的epoch和vote，clear掉原来的votes，并且notify
				 * 			epoch = myepoch: 比较zxid和epoch（注意这里的zxid是之前的，预示着当前server的数据更新情况），根据package类型看是否需要返回结果
				 * 			epoch < myepoch: 丢弃对方的vote，返回No ack package
				 *      b. 如果对方是follower状态：
				 *          epoch > myepoch: 判断是否epoch == myepoch + 1，说明已经有结果了，判断leader是否之前有发送过选票过来，如果有，就更新状态，如果没有，向其ack
				 *          				 如果不是，需要更新自己的epoch，clear掉原来的votes，并且notify
				 * 			epoch = myepoch: 丢弃对方的vote，返回No ack package
				 * 			epoch < myepoch: 丢弃对方的vote，返回No ack package
				 * 
				 * 		c. 如果对方是leader状态：
				 * 			epoch > myepoch: 判断是否epoch == myepoch + 1，说明已经有结果了，直接更新状态
				 * 							 如果不是，需要更新自己的epoch，clear掉原来的votes，并且notify
				 * 			epoch = myepoch: 丢弃对方的vote，返回No ack package
				 * 			epoch < myepoch: 丢弃对方的vote，返回No ack package
				 * 
				 * 2. 如果当前为follower状态：
				 * 		a. 如果对方为looking状态：
				 * 			epoch > myepoch: 认为已经开始新一轮的竞选，将自己状态置为looking状态，notify
				 * 			epoch = myepoch: 认为已经开始新一轮的竞选，将自己状态置为looking状态，notify （不可能）
				 * 			epoch < myepoch: 返回No ack package
				 * 
				 * 		b. 如果对方为follower状态：
				 * 			epoch == myepoch：不做应答
				 *          epoch > myepoch: 当前纪元有问题，需要重新选举
				 *          epoch < myepoch: 返回No ack package
				 *          
				 *      c. 如果对方为leader状态：
				 * 			epoch == myepoch：不做应答
				 *          epoch > myepoch: 当前纪元有问题，需要重新选举
				 *          epoch < myepoch: 返回No ack package
				 *          
				 * 3. 如果昂前为leader状态：
				 * 		a. 如果对方为looking状态：
				 * 			epoch > myepoch: 认为已经开始新一轮的竞选，将自己状态置为looking状态，notify
				 * 			epoch = myepoch: 认为已经开始新一轮的竞选，将自己状态置为looking状态，notify
				 * 			epoch < myepoch: 返回No ack package
				 * 
				 * 		b. 如果对方为follower状态：
				 * 			epoch == myepoch：不做应答
				 *          epoch > myepoch: 当前纪元有问题，需要重新选举
				 *          epoch < myepoch: 返回No ack package
				 *          
				 *      c. 如果对方为leader状态：
				 * 			这里发生冲突，需要重新选举
				 * 
				 */
				
				switch(myStatus)
				{
				
				/**
				 * 如果当前处于Looking状态
				 */
				case QuorumConstant.SERVER_TYPE_LOOKING:
									
					switch(serverStatus)
					{					
					/**
					 * 如果是looking状态：
					 * 
					 * 		a. 如果对方为looking状态：
					 * 			epoch > myepoch: 则我要更新自己的epoch和vote，clear掉原来的votes，并且notify
					 * 			epoch = myepoch: 比较zxid和epoch（注意这里的zxid是之前的，预示着当前server的数据更新情况），根据package类型看是否需要返回结果
					 * 			epoch < myepoch: 丢弃对方的vote，返回No ack package
					 *      b. 如果对方是follower状态：
					 *          epoch > myepoch: 判断是否epoch == myepoch + 1，说明已经有结果了，判断leader是否之前有发送过选票过来，如果有，就更新状态，如果没有，向其ack
					 *          				 如果不是，需要更新自己的epoch，clear掉原来的votes，并且notify
					 * 			epoch = myepoch: 丢弃对方的vote，返回No ack package
					 * 			epoch < myepoch: 丢弃对方的vote，返回No ack package
					 * 
					 * 		c. 如果对方是leader状态：
					 * 			epoch > myepoch: 判断是否epoch == myepoch + 1，说明已经有结果了，直接更新状态
					 * 							 如果不是，需要更新自己的epoch，clear掉原来的votes，并且notify
					 * 			epoch = myepoch: 丢弃对方的vote，返回No ack package
					 * 			epoch < myepoch: 丢弃对方的vote，返回No ack package
					 */
					case QuorumConstant.SERVER_TYPE_LOOKING:
						
						if (myEpoch < voteEpoch)
						{
							/**说明当前的epoch是旧的*/
							qm.setMyEpoch(voteEpoch);
							createCurrentVote();
							notifyAllServer(true);
						}
						else if (myEpoch == voteEpoch)
						{
							updateSelectVotes(vote);
							boolean isChangeVote = updateVote(vote);
							if (isChangeVote)
							{
								notifyAllServer(true);
							}
							else if (spk.isNeedAck())
							{
								PackageBase ackPk = generateSelectPackage(vote.voterId, QuorumConstant.SELECT_PACKAGE_NO_ACK);
								sendPackage(vote.voterId, ackPk);
							}
						}
						else
						{
							removeSelectVotes(vote.voterId);
							PackageBase ackPk = generateSelectPackage(vote.voterId, QuorumConstant.SELECT_PACKAGE_NO_ACK);
							sendPackage(vote.voterId, ackPk);
						}
						break;
					
					case QuorumConstant.SERVER_TYPE_FOLLOWER:
					case QuorumConstant.SERVER_TYPE_LEADER:
						
						if (myEpoch < voteEpoch)
						{
							if (myEpoch + 1 == voteEpoch)
							{
								updateSelectVotes(vote);
								boolean isChangeVote = updateVote(vote);
								/**可能与自己一致，所以不能只判断isChangeVote的情况*/
								if (isChangeVote || currentVote.selectId == vote.selectId)
								{
									if (serverStatus == QuorumConstant.SERVER_TYPE_LEADER)
									{	
										/**如果是leader，就自己finish selection*/
										finishedSelectLeader(vote.selectId);
									}
									else
									{
										/**如果是follower，就要判断leader是否发过vote给自己*/
										Vote leadVote = getSelectVotes(vote.selectId);
										if (leadVote != null && leadVote.selectId == vote.selectId)
										{
											finishedSelectLeader(vote.selectId);
										}
										else
										{
											if(isChangeVote)
											{
												notifyAllServer(true);
											}
											else
											{
												/**给leader server发送ack包*/
												PackageBase ackPk = generateSelectPackage(vote.selectId, QuorumConstant.SELECT_PACKAGE_NEED_ACK);
												sendPackage(vote.selectId, ackPk);
											}
										}
									}
								}
								else
								{
									/**follower or leader的选票还不如自己的，把自己的结果返回*/
									PackageBase ackPk = generateSelectPackage(vote.voterId, QuorumConstant.SELECT_PACKAGE_NO_ACK);
									sendPackage(vote.voterId, ackPk);
								}			
							}
							else
							{
								/**说明我已经落后了好几个纪元了*/
								qm.setMyEpoch(vote.selectEpoch);
								createCurrentVote();
								clearSelectVotes();
								notifyAllServer(true);
							}
						}
						/**这里认为follower是无效的，将自己的信息返回*/
						else
						{
							removeSelectVotes(vote.voterId);
							PackageBase ackPk = generateSelectPackage(vote.voterId, QuorumConstant.SELECT_PACKAGE_NO_ACK);
							sendPackage(vote.voterId, ackPk);
						}					
					
						break;									
					}
					
					break;
				
				/**
				 * 2. 如果当前为follower or leader状态：
				 * 		a. 如果对方为looking状态：
				 * 			epoch > myepoch: 认为已经开始新一轮的竞选，将自己状态置为looking状态，notify
				 * 			epoch = myepoch: 认为已经开始新一轮的竞选，将自己状态置为looking状态，notify （不可能）
				 * 			epoch < myepoch: 判断对方的选择是否优于自己，返回No ack package
				 * 
				 * 		b. 如果对方为follower状态：
				 * 			epoch == myepoch：不做应答
				 *          epoch > myepoch: 当前纪元有问题，需要重新选举
				 *          epoch < myepoch: 返回No ack package
				 *          
				 *      c. 如果对方为leader状态：
				 *      	自己为
				 * 			epoch == myepoch：不做应答
				 *          epoch > myepoch: 当前纪元有问题，需要重新选举
				 *          epoch < myepoch: 返回No ack package
				 */
				case QuorumConstant.SERVER_TYPE_FOLLOWER:				
				case QuorumConstant.SERVER_TYPE_LEADER:
					switch(serverStatus)
					{
					case QuorumConstant.SERVER_TYPE_LOOKING:
						
						if (myEpoch <= voteEpoch)
						{
							qm.setMyEpoch(voteEpoch);
							qm.setMyServerType(QuorumConstant.SERVER_TYPE_LOOKING);
							startSelection();
							notifyAllServer(true);
						}
						else
						{
							if (myEpoch == voteEpoch + 1)
							{
								boolean isChangeVote = updateVote(vote);
								if (isChangeVote)
								{
									qm.increaseMyEpoch();
									startSelection();
									notifyAllServer(true);
								}
								else if (spk.isNeedAck())
								{
									PackageBase ackPk = generateSelectPackage(vote.voterId, QuorumConstant.SELECT_PACKAGE_NO_ACK);
									sendPackage(vote.voterId, ackPk);
								}
								continue;
							}
							/**对方的epoch不正常，返回当前的状态*/
							else
							{
								PackageBase ackPk = generateSelectPackage(vote.voterId, QuorumConstant.SELECT_PACKAGE_NO_ACK);
								sendPackage(vote.voterId, ackPk);
							}
						}
						break;
						
					case QuorumConstant.SERVER_TYPE_FOLLOWER:
					case QuorumConstant.SERVER_TYPE_LEADER:
						
						if (myEpoch < voteEpoch)
						{
							/**当前的*/
							qm.setMyEpoch(vote.selectEpoch);
							qm.setMyServerType(QuorumConstant.SERVER_TYPE_LOOKING);
							startSelection();
							notifyAllServer(true);
						}
						else if (myEpoch == voteEpoch)
						{
							if (currentVote.selectId != vote.selectId)
							{
								qm.increaseMyEpoch();
								qm.setMyServerType(QuorumConstant.SERVER_TYPE_LOOKING);
								startSelection();
								notifyAllServer(true);
							}
							else
							{
								/**如果对方的zxid和epoch发生了改变，也要进行notify*/
								if (!currentVote.challengeVote(vote))
								{
									this.updateCurrentVote();
									notifyAllServer(false);
								}
							}
						}
						else
						{
							PackageBase ackPk = generateSelectPackage(vote.voterId, QuorumConstant.SELECT_PACKAGE_NO_ACK);
							sendPackage(vote.voterId, ackPk);
						}
						
						break;
					}
					break;		
				}
			}	
		}
		catch(InterruptedException e)
		{
			e.printStackTrace();
		}
	}
}
