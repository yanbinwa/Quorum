package org.com.quorum;

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
 * START_WORK:	leader向follower发送的开始工作的信息
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
 * START_WORK: tag + leaderId + leaderEpoch + leaderZxid
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
 * 
 * 
 */

public abstract class QuorumSync implements Runnable {

	public abstract void startUp();
	
}
