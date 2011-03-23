/**
 * 
 */
package com.weibo.jbinlog;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

import com.mysql.jdbc.LogEvent;
import com.mysql.jdbc.MysqlReplicationConn;

/**
 * @author tangfulin
 * 
 */
public class Parser {

	/**
	 * @param args
	 * @throws IOException
	 * @throws SQLException
	 */
	public static void main(final String[] args) throws IOException,
			SQLException {
		final Properties info = new Properties();
		info.put("user", "root");
		info.put("password", "asdf1234");
		info.put("database", "test");

		final MysqlReplicationConn conn = new MysqlReplicationConn("localhost",
				3306, info);
		conn.setBinlogFilename("mysql-bin.000001");
		conn.setBinlogStartPos(106);
		conn.setSlaveId(127);

		conn.startReplicaCommand();

		while (true) {
			final LogEvent event = conn.getNext();
			System.out.println(event);
			if (event == LogEvent.ENUM_END_EVENT) {
				break;
			}
		}
	}
}
