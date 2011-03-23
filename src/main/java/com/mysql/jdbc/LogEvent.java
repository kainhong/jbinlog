/**
 * 
 */
package com.mysql.jdbc;

/**
 * @author tangfulin
 * 
 */
public enum LogEvent {

	UNKNOWN_EVENT,

	START_EVENT_V3,

	QUERY_EVENT,

	STOP_EVENT,

	ROTATE_EVENT,

	INTVAR_EVENT,

	LOAD_EVENT,

	SLAVE_EVENT,

	CREATE_FILE_EVENT,

	APPEND_BLOCK_EVENT,

	EXEC_LOAD_EVENT,

	DELETE_FILE_EVENT,

	NEW_LOAD_EVENT,

	RAND_EVENT,

	USER_VAR_EVENT,

	FORMAT_DESCRIPTION_EVENT,

	XID_EVENT,

	BEGIN_LOAD_QUERY_EVENT,

	EXECUTE_LOAD_QUERY_EVENT,

	TABLE_MAP_EVENT,

	PRE_GA_WRITE_ROWS_EVENT,

	PRE_GA_UPDATE_ROWS_EVENT,

	PRE_GA_DELETE_ROWS_EVENT,

	WRITE_ROWS_EVENT,

	UPDATE_ROWS_EVENT,

	DELETE_ROWS_EVENT,

	INCIDENT_EVENT,

	HEARTBEAT_LOG_EVENT,

	ENUM_END_EVENT;

	private String dbName;
	private String tableName;
	private String binlogName;
	private Long binlogPos;
	private String[] data;
	private String[] oldData;

	public String getDbName() {
		return dbName;
	}

	public void setDbName(final String dbName) {
		this.dbName = dbName;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(final String tableName) {
		this.tableName = tableName;
	}

	public String getBinlogName() {
		return binlogName;
	}

	public void setBinlogName(final String binlogName) {
		this.binlogName = binlogName;
	}

	public Long getBinlogPos() {
		return binlogPos;
	}

	public void setBinlogPos(final Long binlogPos) {
		this.binlogPos = binlogPos;
	}

	public void setData(final String[] data) {
		this.data = data;
	}

	public String[] getData() {
		return data;
	}

	public void setOldData(final String[] oldData) {
		this.oldData = oldData;
	}

	public String[] getOldData() {
		return oldData;
	}

	// ///////////

	public int getId() {
		return ordinal();
	}

	public static LogEvent valueOf(final int id) {
		for (final LogEvent ev : LogEvent.values()) {
			if (ev.getId() == id) {
				return ev;
			}
		}
		return UNKNOWN_EVENT;
	}

}
