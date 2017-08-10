package org.com.quorum;

import org.com.connection.SessionManager;
import org.com.connection.SyncConnectionLeader;

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
 * HEAT_BEAT: follower与leader的session控制
 * 
 * 
 * 监听sync端口，建立与follower的连接
 * 
 * 1. 接受follower的sync_up包，并接收sync_up_commit包，一旦超过1/2，就可以向目前连接的follower发送start work，之后
 * 如果还接收到sync_up_commit包，直接返回start work
 * 
 * 2. 接收到follower的request包，向当前连接follower发送prompt包，这里只设置一个定时任务，对所有的request进行遍历，按照
 * recordID来依次处理（这里有一个request相应时间，如果在该时间内没有达到1/2的结果，则放弃），对于record执行的顺序必须是按顺序
 * 来执行的。
 * 
 * 3. 如果一旦commit，leader先向follower发送work commit，并且执行该record，更新自己的zxid
 * 
 * 4. 如果follower的连接断开，并且有操过1/2的follower失联，则认为失败
 *
 */

public class QuorumSyncLeader extends QuorumSync {

	Quorum qm = null;
	SessionManager sessionManager = null;
	SyncConnectionLeader scl = null;
	
	public QuorumSyncLeader(Quorum quorum)
	{
		qm = quorum;
	}
	
	public void startUp()
	{
		
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		
	}
	
}
