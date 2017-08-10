package org.com.constant;

public class QuorumConstant {
	
	public static final int SERVER_TYPE_LOOKING = 0;
	public static final int SERVER_TYPE_LEADER = 1;
	public static final int SERVER_TYPE_FOLLOWER = 2;
	
	public static final int SELECT_PACKAGE_NEED_ACK = 0;
	public static final int SELECT_PACKAGE_NO_ACK = 1;
		
	public static final int SELECT_ACK_SERVERID_ACK = Integer.MIN_VALUE;
	public static final int SELECT_TEST_TAG = Integer.MIN_VALUE + 1;
	
	public static final int SYNC_SNYC_UP = 0;
	public static final int SYNC_SNYC_BACK = 1;
	public static final int SYNC_START_WORK = 2;
	public static final int SYNC_WORK_REQUEST = 3;
	public static final int SYNC_WORK_PROMOTE = 4;
	public static final int SYNC_WORK_REPLY = 5;
	public static final int SYNC_WORK_COMMIT = 6;
	public static final int SYNC_WORK_REJECT = 7;
	public static final int SYNC_SNYC_UP_COMMIT = 8;
	public static final int SYNC_START_WORK_COMMIT = 9;
	
//	public static final int[] SYNC_WAIT_TO_SYNC_LIST= {SYNC_SNYC_UP, SYNC_SNYC_BACK, SYNC_SNYC_UP_COMMIT};
//	public static final int[] SYNC_WAIT_TO_START_LIST = {SYNC_START_WORK, SYNC_START_WORK_COMMIT};
//	public static final int[] SYNC_WORKING_LIST = {SYNC_WORK_REQUEST, SYNC_WORK_PROMOTE, SYNC_WORK_REPLY, 
//											SYNC_WORK_COMMIT, SYNC_WORK_REJECT};
	
	public static final int RECORD_ADD = 0;
	public static final int RECORD_UPDATE = 1;
	public static final int RECORD_DELETE = 2;
	
	public static final int VALUE_INT = 0;
	public static final int VALUE_LONG = 1;
	public static final int VALUE_STINRG = 2;
	
	public static final int SESSION_DELAY = 5000;
	public static final int CLIENT_INTERVAL = 60;	//1MIN
	public static final int SYNC_INTERVAL = 60;		//1MIN
	
	public static final int FOLLOWER_WAIT_TO_SYNC = 0;
	public static final int FOLLOWER_WAIT_TO_START = 1;
	public static final int FOLLOWER_WORKING = 2;
}
