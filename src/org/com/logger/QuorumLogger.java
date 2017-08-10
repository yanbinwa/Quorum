package org.com.logger;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

/**提供全局的logger*/

public class QuorumLogger {
	
	static QuorumLogger quorumLooger = null;
	public MyLogger logger = null;
	private SimpleDateFormat format = null;
	
	public static QuorumLogger getInstance()
	{
		if (quorumLooger == null)
		{
			quorumLooger = new QuorumLogger();
		}
		return quorumLooger;
	}
	
//	private QuorumLogger() 
//	{
//		logger = Logger.getLogger("QuorumLogger");
//		ConsoleHandler consoleHandler = new ConsoleHandler();
//		consoleHandler.setFormatter(new MyLogHander());
//		logger.addHandler(consoleHandler);
//	}
	
	private QuorumLogger() 
	{
		logger = new MyLogger();
		format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
	}
	
	private String getDate()
	{
		return format.format(new Date());
	}
	
	class MyLogHander extends Formatter
	{

		@Override
		public String format(LogRecord record) {
			// TODO Auto-generated method stub
//			String[] classNames = record.getSourceClassName().split(".");
//			String className = classNames[classNames.length - 1];
			return String.format("%s | %s | %s ", 
					record.getSourceClassName(), record.getSourceMethodName(), 
					record.getLevel());
		}
		
	}
	
	public class MyLogger
	{
		public void info(Object msg)
		{
			String dateStr = getDate();
			System.out.println(dateStr + " | " + "INFO | " + msg);
		}
		
		public void warning(Object msg)
		{
			String dateStr = getDate();
			System.out.println(dateStr + " | " + "WARNING | " + msg);
		}
		
		public void severe(Object msg)
		{
			String dateStr = getDate();
			System.out.println(dateStr + " | " + "ERROR | " + msg);
		}
	}
}
