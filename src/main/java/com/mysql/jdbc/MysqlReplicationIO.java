/**
 * 
 */
package com.mysql.jdbc;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tangfulin
 * 
 */
public class MysqlReplicationIO extends MysqlIO {

	@SuppressWarnings("unused")
	private static final Logger log = LoggerFactory
			.getLogger(MysqlReplicationIO.class);

	// ///////////////////

	public MysqlReplicationIO(final String host, final int port,
			final Properties props, final MySQLConnection conn)
			throws IOException, SQLException {
		super(host, port, props, StandardSocketFactory.class.getName(), conn,
				3000, 1024 * 1024);
	}

	public boolean sendCommand(final int command, final Buffer buffer)
			throws SQLException, IOException {
		final int len = buffer.getPosition();
		buffer.setPosition(0);
		buffer.writeLongInt(len - HEADER_LENGTH);
		buffer.writeByte((byte) 0);
		this.mysqlOutput.write(buffer.getBytes(0, len));
		this.mysqlOutput.flush();
		// skip the first reply
		// checkErrorPacket();
		return true;
	}

	public Buffer getNextReply() throws SQLException, IOException {
		return checkErrorPacket();
	}

	/**
	 * @param args
	 */
	public static void main(final String[] args) {
		final int value = (0x0f & 0xff) | ((0x01 & 0xff) << 8);
		System.out.println(value);
	}

}
