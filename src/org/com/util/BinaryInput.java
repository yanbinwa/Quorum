package org.com.util;

import java.io.IOException;

public abstract class BinaryInput {
	
	public abstract long readLong() throws IOException;
	public abstract int readInt() throws IOException;
	public abstract boolean readBoolean() throws IOException;
	public abstract byte[] readByte(int len) throws IOException;
}
