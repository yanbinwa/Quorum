package org.com.util;

import java.io.IOException;

public abstract class BinaryOutput {
	
	public abstract void writeLong(long value) throws IOException;
	public abstract void writeInt(int value) throws IOException;
	public abstract void writeBoolean(boolean value) throws IOException;
	public abstract void writeByte(byte[] buffer) throws IOException;
}
