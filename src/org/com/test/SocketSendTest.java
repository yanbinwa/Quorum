package org.com.test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import org.com.util.BinaryInputStream;
import org.com.util.BinaryOutputStream;

public class SocketSendTest {
	
	Socket socket = null;
	
	boolean isSendConnectRequest = false;
	
	boolean isBuildSock = false;
	
	LinkedBlockingQueue<Integer> queue = null;
	
	int buffer = -1;
	
	
	
	private void sendInteger(int data) throws InterruptedException
	{
		if (!isBuildSock)
		{
			buffer = data;
			Thread thread = new Thread(
					new Runnable()
					{

						@Override
						public void run() {
							// TODO Auto-generated method stub
							requestConnect();
						}
						
					});
			thread.start();
		}
		else
		{
			queue.put(data);
		}
	}
	
	private void requestConnect()
	{
		try
		{
			if (isSendConnectRequest)
			{
				return;
			}
			else
			{
				isSendConnectRequest = true;
			}
			System.out.println("requestConnect");
			socket = new Socket();
			SocketReceiveTest.setSockOpts(socket);
			socket.connect(new InetSocketAddress("127.0.0.1", 8080), 1000);
			queue = new LinkedBlockingQueue<Integer>();
			System.out.println("Build socket success");
			isBuildSock = true;
			BinaryOutputStream bos = new BinaryOutputStream(socket.getOutputStream());
			while(true)
			{
				int data = queue.take();
				bos.writeInt(data);
			}
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
			isSendConnectRequest = false;
			try
			{
				socket.close();
			}
			catch(Exception e1)
			{
				System.out.println(e1.getMessage());
			}
		}
	}
	
	public void startUp() throws InterruptedException
	{
		int i = 10000;
		Thread thread = new Thread(
			new Runnable()
			{

				@Override
				public void run() {
					// TODO Auto-generated method stub
					while(true)
					{
						try 
						{
							if(buffer != -1)
							{
								int tmp = buffer;
								buffer = -1;
								sendInteger(tmp);
								
							}
							Thread.sleep(1000);
						} 
						catch (InterruptedException e) 
						{
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						
					}
				}
				
		});
		thread.start();
		sendInteger(i);
	}
	
	
	public static void main(String[] args) throws UnknownHostException, IOException, InterruptedException
	{
		SocketSendTest sst = new SocketSendTest();
		sst.startUp();
		int i = 100;
		while(true)
		{
			System.out.println("write int: " + i);
			sst.sendInteger(i ++);
			Thread.sleep(1000);
		}
	}
}
