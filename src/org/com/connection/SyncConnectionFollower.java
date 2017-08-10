package org.com.connection;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.com.exception.DeSerizliationException;
import org.com.exception.SerizliationException;
import org.com.logger.QuorumLogger;
import org.com.packagebase.PackageBase;
import org.com.packagebase.SyncPackage;
import org.com.quorum.Quorum;
import org.com.quorum.QuorumSyncFollower;
import org.com.util.BinaryInputStream;
import org.com.util.BinaryOutputStream;

public class SyncConnectionFollower extends Connection {

	Quorum qm = null;
	QuorumSyncFollower asf = null;
	int port;
	Socket socket = null;
	ConnectThread connectThread = null;
	LinkedBlockingQueue<PackageBase> syncReceiveQueue = null;
	LinkedBlockingQueue<PackageBase> syncSendQueue = null;
	QuorumLogger qml = null;
	AtomicBoolean isConnectThreadRequest = new AtomicBoolean();
	
	public SyncConnectionFollower(Quorum qm, QuorumSyncFollower asf, int port)
	{
		this.qm = qm;
		this.asf = asf;
		this.port = port;
		this.syncReceiveQueue = new LinkedBlockingQueue<PackageBase>();
		this.syncSendQueue = new LinkedBlockingQueue<PackageBase>();
		isConnectThreadRequest.set(false);
		qml = QuorumLogger.getInstance();
	}
	
	public void startUp()
	{
		/*Does not do any thing*/
	}
	
	public void deleteConnectThread(int id)
	{
		connectThread.stop();
		connectThread = null;
	}
	
	/**这里也存在多次request的问题*/
	public boolean sendPackage(PackageBase pk)
	{
		if (connectThread == null)
		{
			/**这里就要发起连接了*/
			if (isConnectThreadRequest.get())
			{
				return false;
			}
			isConnectThreadRequest.set(true);
			Thread thread = new Thread(
					new Runnable() {

						@Override
						public void run() {
							// TODO Auto-generated method stub
							requestConnect();
						}
						
					});
			thread.start();
			return false;
		}
		try 
		{
			syncSendQueue.put(pk);
			return true;
		} 
		catch (InterruptedException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}		
	}
	
	public void requestConnect()
	{
		int leaderId = qm.getLeaderId();
		if (leaderId == qm.getMyId())
		{
			qml.logger.severe("the follower id is equeal with leader id");
			return;
		}
		String ip = qm.getServerIp(leaderId);
		if (ip == null)
		{
			qml.logger.severe("the leader ip is null");
			return;
		}
		socket = new Socket();
		try 
		{
			socket.connect(new InetSocketAddress(ip, this.port), 1000);
			connectThread = new ConnectThread(socket);
			isConnectThreadRequest.set(false);
			connectThread.startUp();
			asf.syncConnectReady();
		} 
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
			try 
			{
				socket.close();
			} 
			catch (IOException e1) 
			{
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}	
			isConnectThreadRequest.set(false);
		}
	}
	
	public PackageBase getPackage()
	{
		try 
		{
			PackageBase pk = syncReceiveQueue.take();
			return pk;
		} 
		catch (InterruptedException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}
	
	class ConnectThread
	{
		int serverId;
		boolean finished = false;
		Socket socket = null;
		BinaryInputStream bis = null;
		BinaryOutputStream bos = null;
		Thread writeThread = null;
		Thread readThread = null;
		
		public ConnectThread(Socket socket)
		{
			this.socket = socket;

			try 
			{
				bis = new BinaryInputStream(socket.getInputStream());
				bos = new BinaryOutputStream(socket.getOutputStream());
			} 
			catch (IOException e) 
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		public void startUp()
		{
			readThread = new Thread(
					new Runnable(){

						@Override
						public void run() {
							// TODO Auto-generated method stub
							readProcess();
						}
						
					});
			
			readThread.start();
			
			writeThread = new Thread(
					new Runnable(){

						@Override
						public void run() {
							// TODO Auto-generated method stub
							writeProcess();
						}
						
					});
			
			writeThread.start();
		}
		
		public void shutDown()
		{
			try 
			{
				socket.close();
			} 
			catch (IOException e) 
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		public void stop()
		{
			writeThread.interrupt();
			readThread.interrupt();
			shutDown();
			finished = true;
		}
		
		private void readProcess()
		{
			while(!finished)
			{
				SyncPackage pk = new SyncPackage();
				try 
				{
					synchronized(bis)
					{
						int bufferLen = bis.readInt();
						byte[] buffer = bis.readByte(bufferLen);
						pk.deSerizliation(buffer);
					}
					syncReceiveQueue.put(pk);
				} 
				catch (DeSerizliationException e) 
				{
					qml.logger.severe("step2: id is: " + this.serverId + " " + e.getMessage());
					this.shutDown();
					return;
				} 
				catch (InterruptedException e) 
				{
					// TODO Auto-generated catch block
					qml.logger.severe("step3: id is: " + this.serverId + " " + e.getMessage());
				} 
				catch (IOException e) 
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
		private void writeProcess()
		{
			while(!finished)
			{
				try 
				{
					PackageBase pk = syncSendQueue.take();
					synchronized(bos)
					{
						byte[] buffer = pk.serizlization();
						bos.writeInt(buffer.length);
						bos.writeByte(buffer);
					}				
				} 
				catch (SerizliationException e) 
				{
					// TODO Auto-generated catch block
					qml.logger.severe("step4: id is: " + this.serverId + " " + e.getMessage());
					this.shutDown();
					return;
				} 
				catch (InterruptedException e) 
				{
					// TODO Auto-generated catch block
					qml.logger.severe("step5: id is: " + this.serverId + " " + e.getMessage());
				} 
				catch (IOException e) 
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
	}

	@Override
	public void sendHeatBeat(long sessionId) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void sessionTimeout() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void sessionTimeout(long sessionID) {
		// TODO Auto-generated method stub
		
	}
}
