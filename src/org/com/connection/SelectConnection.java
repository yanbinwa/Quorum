package org.com.connection;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.com.constant.QuorumConstant;
import org.com.exception.DeSerizliationException;
import org.com.exception.SerizliationException;
import org.com.logger.QuorumLogger;
import org.com.packagebase.PackageBase;
import org.com.packagebase.SelectPackage;
import org.com.quorum.Quorum;
import org.com.util.BinaryInputStream;
import org.com.util.BinaryOutputStream;

/**
 * 每当Accept获取连接时，先尝试创建一个connection，读取server id，如果id小于myId，停止该
 * connection. 如果相反，保存该connection
 *
 */

public class SelectConnection {
	
	Quorum qm = null;
	int port = 0;
	AcceptThread accptThread = null;
	
	Map<Integer, ConnectThread> connectThreads = null;
	Map<Integer, Boolean> isSendConnectRequest = null;
	
	/**存放需要发送的数据，这里要区分*/
	Map<Integer, LinkedBlockingQueue<PackageBase>> selectionSendQueueMap = null;
	
	/**保存接受到的package，由QuorumSelection消费*/
	LinkedBlockingQueue<PackageBase> selectionReceiveQueue = null;
	
	QuorumLogger qml = null;
	
	
	public SelectConnection(Quorum qm, int port)
	{
		this.qm = qm;
		this.port = port;
		
		selectionSendQueueMap = new HashMap<Integer, LinkedBlockingQueue<PackageBase>>();
		selectionReceiveQueue = new LinkedBlockingQueue<PackageBase>();
		connectThreads = new HashMap<Integer, ConnectThread>();
		isSendConnectRequest = new HashMap<Integer, Boolean>();
		qml = QuorumLogger.getInstance();
	}
	
	public void startUp()
	{
		accptThread = new AcceptThread();
		new Thread(accptThread).start();
	}
	
	public PackageBase getPackage() throws InterruptedException
	{
		return selectionReceiveQueue.take();
	}
	
	public boolean checkConnectionIsCreate(int id)
	{
		ConnectThread connectThread = null;
		synchronized(connectThreads)
		{
			connectThread = connectThreads.get(id);
		}
		if (connectThread == null)
		{
			return false;
		}
		/**判断当前的socket是否有效，如果失效，就将其删除，并返回false*/
		return true;
	}
	
	/**
	 * 先判断是否存在与该id的queue，如果有，直接写入相应的queue中，如果没有，创建ConnectionThread
	 * 创建的过程需要异步来进行
	 * @throws InterruptedException 
	 * 
	 * 如果当前connectThread还没有建立，则将该package写入到queue的末尾，
	 * */
	public boolean sendPackage(int id, PackageBase packageBase) throws InterruptedException
	{
		if (!checkConnectionIsCreate(id)) 
		{
			synchronized(isSendConnectRequest)
			{
				if (isSendConnectRequest.containsKey(id) && isSendConnectRequest.get(id) == true)
				{
					return false;
				}
				else
				{
					isSendConnectRequest.put(id, true);
				}
			}
			Thread thread = new Thread(
					new Runnable() {

						@Override
						public void run() {
							// TODO Auto-generated method stub
							requestConnect(id);
						}
						
					});
			thread.start();
			return false;
		}
		else
		{
			selectionSendQueueMap.get(id).put(packageBase);
			return true;
		}
	}
	
	/**这里防止对一个id进行多次的socket请求*/
	public void requestConnect(int id)
	{
		Socket socket = null;
		if (checkConnectionIsCreate(id))
		{
			return;
		}
		String ip = qm.getServerIp(id);
		if (ip == null)
		{
			qml.logger.info("The ip of server " + id + " is null");
			return;
		}
		try 
		{
			socket = new Socket();
			socket.connect(new InetSocketAddress(ip, this.port), 1000);
			BinaryOutputStream bos = new BinaryOutputStream(socket.getOutputStream());
			bos.writeInt(qm.getMyId());
			BinaryInputStream bis = new BinaryInputStream(socket.getInputStream());
			int serverId = bis.readInt();
			bos.writeInt(QuorumConstant.SELECT_ACK_SERVERID_ACK);
			qml.logger.info("requestConnect: MyId is " + qm.getMyId() + " ServerId is " + serverId);
			/**
			 * 如果对方id比我的id大，说明对方会主动连我，我只要close掉当前的socket即可
			 * 如果相反，我就讲
			 */
			if (serverId > qm.getMyId())
			{
				socket.close();
			}
			else
			{
				createConnectThread(socket, serverId);
			}
			
		} 
		catch (Exception e) 
		{
			try 
			{
				socket.close();
				synchronized(isSendConnectRequest)
				{
					isSendConnectRequest.put(id, false);
				}
			} 
			catch (IOException e1) 
			{
				qml.logger.severe("socket close failed " + e1.getMessage());
			}
			qml.logger.severe("socket connect failed " + e.getMessage());
		}
		finally
		{
			synchronized(isSendConnectRequest)
			{
				isSendConnectRequest.put(id, false);
			}
		}
	}
	
	public void acceptConnect(Socket socket)
	{
		try 
		{
			BinaryInputStream bis = new BinaryInputStream(socket.getInputStream());
			BinaryOutputStream bos = new BinaryOutputStream(socket.getOutputStream());
			int serverId = bis.readInt();
			bos.writeInt(qm.getMyId());
			int ret = bis.readInt();
			if (ret != QuorumConstant.SELECT_ACK_SERVERID_ACK)
			{
				qml.logger.severe("Can not reveice SELECT_ACK_SERVERID_ACK");
			}
			qml.logger.info("acceptConnect: MyId is " + qm.getMyId() + " ServerId is " + serverId);
			if (serverId < qm.getMyId())
			{
				socket.close();
				if (!checkConnectionIsCreate(serverId))
				{
					requestConnect(serverId);
				}
			}
			else
			{
				/**这里要判断之前是否已经*/
				createConnectThread(socket, serverId);
			}
		} 
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}
		
	public void createConnectThread(Socket socket, int id)
	{
		if (checkConnectionIsCreate(id)) 
		{
			qml.logger.info("In createConnectThread, the thread has already build: id is " + id + ", ip is " + socket.getInetAddress().getHostAddress());
			return;
		}
		qml.logger.info("createConnectThread: id is " + id + ", ip is " + socket.getInetAddress().getHostAddress());
		LinkedBlockingQueue<PackageBase> queue = new LinkedBlockingQueue<PackageBase>();
		selectionSendQueueMap.put(id, queue);
		ConnectThread connecThread = new ConnectThread(socket, id);
		connectThreads.put(id, connecThread);
		connecThread.startUp();
	}
	
	public void deleteConnectThread(int id)
	{
		selectionSendQueueMap.remove(id);
		connectThreads.get(id).stop();
		connectThreads.remove(id);
	}

	class AcceptThread implements Runnable
	{
		boolean finished = false;
		ServerSocket server = null;

		@Override
		public void run() {
			// TODO Auto-generated method stub
			try 
			{
				server = new ServerSocket(port);
			} 
			catch (IOException e) 
			{
				// TODO Auto-generated catch block
				qml.logger.severe(e.getMessage());
				return;
			}
			
			while(!finished)
			{
				try 
				{
					Socket socket = server.accept();
					qml.logger.info("service ip is: " + socket.getInetAddress().getHostAddress());
					Thread thread = new Thread(
							new Runnable() 
							{

								@Override
								public void run() {
									// TODO Auto-generated method stub
									acceptConnect(socket);
								}
								
							});
					thread.start();
					
				} 
				catch (IOException e) 
				{
					// TODO Auto-generated catch block
					qml.logger.severe("step1" + e.getMessage());
				}
			}
			
		}
		
	}
	
	/**
	 * hold the input and output flow. 通过selector来控制读和写
	 * 首先会尝试建立连接，如果失败
	 */
	class ConnectThread
	{
		LinkedBlockingQueue<PackageBase> sendQueue = null;
		int serverId;
		boolean finished = false;
		Socket socket = null;
		BinaryInputStream bis = null;
		BinaryOutputStream bos = null;
		Thread writeThread = null;
		Thread readThread = null;
		
		public ConnectThread(Socket socket, int id)
		{
			this.socket = socket;
			this.serverId = id;
			this.sendQueue = selectionSendQueueMap.get(id);
			
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
				connectThreads.remove(this.serverId);
			} 
			catch (IOException e1) 
			{
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
		
		private void readProcess()
		{
			while(!finished)
			{
				SelectPackage pk = new SelectPackage();
				try 
				{
					synchronized(bis)
					{
						int bufferLen = bis.readInt();
						byte[] buffer = bis.readByte(bufferLen);
						pk.deSerizliation(buffer);
					}
					selectionReceiveQueue.put(pk);
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
					PackageBase pk = sendQueue.take();
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
		
		public void stop()
		{
			writeThread.interrupt();
			readThread.interrupt();
			shutDown();
			finished = true;
		}
		
	}
	
}
