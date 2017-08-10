package org.com.test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;

public class SocketReceiveTest {
	/**
	 * 接受socket创建请求，并读取其
	 * @throws SocketException 
	 * @throws IOException 
	 */
	
	public static void setSockOpts(Socket socket) throws SocketException
	{
		socket.setTcpNoDelay(true);
		socket.setSoTimeout(10000);
	}
	
	public static void main(String[] args)
	{
		int port = 8080;
		ServerSocket server = null;
		try {
			server = new ServerSocket(port);				
			while(true)
			{
				Socket socket = server.accept();
				setSockOpts(socket);
				System.out.println("accept socket");
				DataOutputStream bos = new DataOutputStream(socket.getOutputStream());
				DataInputStream bis = new DataInputStream(socket.getInputStream());
				while(true)
				{
					int input = bis.readInt();
					System.out.println(input);
				}
//				int input = bis.readInt();
//				System.out.println(input);
//				input = bis.readInt();
//				System.out.println(input);
//				bos.writeInt(input);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
