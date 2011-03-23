/**
 * 
 */
package com.mysql.jdbc;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.weibo.jbinlog.Parser;

/**
 * @author tangfulin
 * 
 */
public class MysqlReplicationConn extends ConnectionImpl {

	private static final Logger log = LoggerFactory
			.getLogger(MysqlReplicationConn.class);

	private final Properties config;
	private transient MysqlReplicationIO io = null;

	private String binlogFilename;
	private long binlogStartPos;
	private long slaveId;

	private final Map<Long, TableMap> tableMap = new HashMap<Long, TableMap>();

	private final ThreadLocal<Buffer> localBuffer = new ThreadLocal<Buffer>() {
		@Override
		protected Buffer initialValue() {
			return new Buffer(1024);
		}
	};

	public MysqlReplicationConn(final String host, final int port,
			final Properties info) throws SQLException, IOException {
		// super(host, port, info, info.getProperty("database"), null);
		this.setAutoReconnect(true);
		this.setDumpQueriesOnException(true);
		this.setEnablePacketDebug(true);
		this.setLoggerClassName(Logger.class.getName());
		this.setTraceProtocol(true);
		this.setUltraDevHack(true);
		this.setProfileSql(true);
		this.setAutoGenerateTestcaseScript(true);

		this.config = this.exposeAsProperties(info);
		this.io = new MysqlReplicationIO(host, port, this.config, this);
		this.io.doHandshake(this.config.getProperty("user"),
				this.config.getProperty("password"),
				this.config.getProperty("database"));

	}

	/**
	 * binlog file name is like mysql-bin.000021
	 * 
	 * @param fileName
	 */
	public void setBinlogFilename(final String binlogFilename) {
		this.binlogFilename = binlogFilename;
	}

	public long getBinlogStartPos() {
		return binlogStartPos;
	}

	public void setBinlogStartPos(final long binlogStartPos) {
		this.binlogStartPos = binlogStartPos;
	}

	public String getBinlogFilename() {
		return binlogFilename;
	}

	public void setSlaveId(final long slaveId) {
		this.slaveId = slaveId;
	}

	public long getSlaveId() {
		return slaveId;
	}

	// /////////////

	public boolean startReplicaCommand() throws SQLException, IOException {
		final Buffer command = makeDumpBinlogCommand(binlogFilename,
				binlogStartPos);
		this.io.sendCommand(MysqlDefs.COM_BINLOG_DUMP, command);
		return true;

	}

	public LogEvent getNext() throws SQLException, IOException {
		final Buffer result = this.io.getNextReply();
		result.setPosition(0);
		return parseResult(result);
	}

	private LogEvent parseResult(final Buffer result) throws SQLException,
			IOException {
		// debugBuffer(result);

		LogEvent event = LogEvent.ENUM_END_EVENT;

		if (result.isLastDataPacket()) {
			return event;
		}

		final int bufferLen = result.getBufLength();
		if (bufferLen < 8) {
			return event;
		}

		// mysql packet header
		// fieldCount, always 0 here
		final int fieldCount = result.readByte();
		if (fieldCount == 254) {
			return event;
		}

		// now enter log_envent.cc Log_event::read_log_event

		// event header
		final long timestamp = result.readLong();
		final int typeCode = result.readByte();
		final long serverId = result.readLong();
		final long eventLength = result.readLong();
		final long nextPos = result.readLong();
		final int flags = result.readInt();
		// extra header now empty
		if (log.isDebugEnabled()) {
			log.debug("buf len: " + bufferLen + " timestamp:" + timestamp
					+ " typeCode:" + typeCode + " serverId:" + serverId
					+ " eventLength:" + eventLength + " nextPos:" + nextPos
					+ " flags:" + flags);
			// debugBuffer(result);
		}

		if (eventLength != (bufferLen - 1)
				|| typeCode > LogEvent.ENUM_END_EVENT.getId()) {
			throw new IOException("Sanity check failed");
		}

		event = LogEvent.valueOf(typeCode);

		// parse event data
		switch (event) {
		case ROTATE_EVENT:
			final long nextBinlogPos = result.readLongLong();
			final String nextBinlog = result.readString();
			if (timestamp == 0) {
				log.info("fake rotete, continue");
				break;
			}
			this.setBinlogFilename(nextBinlog);
			this.setBinlogStartPos(nextBinlogPos);
			log.info("Rotate binlog to " + nextBinlog + " : " + nextBinlogPos);
			break;
		case TABLE_MAP_EVENT:
			parseTableMapEvent(result);

			break;
		case WRITE_ROWS_EVENT:
			// fall through
		case UPDATE_ROWS_EVENT:
			// fall through
		case DELETE_ROWS_EVENT:
			parseRowEvent(result, event);

			break;
		case INTVAR_EVENT:
			// LAST_INSERT_ID_EVENT==1
			// INSERT_ID_EVENT==2
			final byte type = result.readByte();
			final long value = result.readLongLong();
			if (log.isDebugEnabled()) {
				log.debug("intvar_event: type: " + type + " value:" + value);
			}
			break;
		case STOP_EVENT:
			break;
		default:
			if (log.isDebugEnabled()) {
				log.debug("ignore parse event:" + event);
			}
			break;
		}

		return event;
	}

	private void parseRowEvent(final Buffer result, final LogEvent event)
			throws IOException {
		// uint6korr
		final long tableId = result.readLongInt() & 0xffffff
				| ((result.readLongInt() & 0xffffff) << 24);
		final TableMap tbmap = tableMap.get(tableId);
		if (tbmap == null) {
			throw new IOException("no table map for id:" + tableId);
		}
		final int eventFlags = result.readInt();
		final long width = result.readFieldLength();
		final int bitmapLen = (int) ((width + 7) / 8);
		final byte[] colBitmap = new byte[bitmapLen];
		for (int i = 0; i < bitmapLen; ++i) {
			colBitmap[i] = result.readByte();
		}
		final byte[] colAiBitmap = new byte[bitmapLen];
		if (event == LogEvent.UPDATE_ROWS_EVENT) {
			for (int i = 0; i < bitmapLen; ++i) {
				colAiBitmap[i] = result.readByte();
			}
		}
		final long eventLength = result.getBufLength() - 1;
		final long dataSize = eventLength - (result.getPosition() - 1);

		final int nullBitsLen = (int) ((width + 7) / 8);
		final byte[] nullBits = new byte[nullBitsLen];
		for (int i = 0; i < nullBitsLen; ++i) {
			nullBits[i] = result.readByte();
		}

		if (log.isDebugEnabled()) {
			log.debug("tableId:" + tableId + " evFlag:" + eventFlags
					+ " width:" + width + " dataSize:" + dataSize);
		}

		// now all left in result are my_rows_buf
		debugBuffer(result);
		// see log_event.cc#Rows_log_event::print_verbose_one_row (line
		// 1873)

		for (int i = 0, nullIndex = 0; i < tbmap.getColCount(); ++i) {
			if (!bitMapIsSet(colBitmap, i)) {
				log.warn("bitmap not set, ignore col " + i);
				continue;
			}

			if (bitMapIsSet(nullBits, nullIndex)) {
				log.info("col null : " + nullIndex);
				nullIndex++;
				continue;
			}
			nullIndex++;

			final int type = tbmap.getColType()[i];
			final int meta = tbmap.getMetadata()[i];
			final long longValue;
			final double doubleValue;
			final String strValue;

			switch (type) {
			case MysqlDefs.FIELD_TYPE_TINY:
				longValue = result.readByte();
				strValue = String.valueOf(longValue);
				break;
			case MysqlDefs.FIELD_TYPE_SHORT:
				longValue = result.readInt();
				strValue = String.valueOf(longValue);
				break;
			case MysqlDefs.FIELD_TYPE_INT24:
				// fall through
			case MysqlDefs.FIELD_TYPE_TIME:
				longValue = result.readLongInt();
				strValue = String.valueOf(longValue);
				break;
			case MysqlDefs.FIELD_TYPE_LONG:
				// fall through
			case MysqlDefs.FIELD_TYPE_TIMESTAMP:
				longValue = result.readLong();
				strValue = String.valueOf(longValue);
				break;
			case MysqlDefs.FIELD_TYPE_YEAR:
				longValue = result.readLong() + 1900;
				strValue = String.valueOf(longValue);
				break;
			case MysqlDefs.FIELD_TYPE_LONGLONG:
				// fall through
			case MysqlDefs.FIELD_TYPE_DATETIME:
				longValue = result.readLongLong();
				strValue = String.valueOf(longValue);
				break;
			case MysqlDefs.FIELD_TYPE_FLOAT:
				doubleValue = Float.intBitsToFloat((int) result.readLong());
				strValue = String.valueOf(doubleValue);
				break;
			case MysqlDefs.FIELD_TYPE_DOUBLE:
				doubleValue = Double.longBitsToDouble(result.readLongLong());
				strValue = String.valueOf(doubleValue);
				break;

			case MysqlDefs.FIELD_TYPE_VARCHAR:
				// fall through
			case MysqlDefs.FIELD_TYPE_VAR_STRING:
				// fall through
			case MysqlDefs.FIELD_TYPE_STRING:

			case MysqlDefs.FIELD_TYPE_NEW_DECIMAL:
				// fall through
			case MysqlDefs.FIELD_TYPE_BLOB:
				// fall through
			case MysqlDefs.FIELD_TYPE_GEOMETRY:
				// fall through
			case MysqlDefs.FIELD_TYPE_BIT:
				// TODO support it !
				throw new UnsupportedOperationException("unsupported type : "
						+ MysqlDefs.typeToName(type));

			default:
				break;
			}

		}
	}

	private void parseTableMapEvent(final Buffer result) {
		// fill tableMap
		final long tId = result.readLongInt() & 0xffffff
				| ((result.readLongInt() & 0xffffff) << 24);
		// reserved flags
		result.readInt();

		TableMap tmap = tableMap.get(tId);
		if (tmap == null) {
			tmap = new TableMap();
			tableMap.put(tId, tmap);
		}

		final String database = readNullTerminatedString(result);
		final String table = readNullTerminatedString(result);
		final int colCount = (int) result.readFieldLength();
		final int[] colType = new int[colCount];
		for (int i = 0; i < colCount; i++) {
			colType[i] = result.readByte();
		}
		// metadata
		final int metadataLen = (int) result.readFieldLength();
		final int[] metadata = new int[colCount];
		int j = 0;
		for (int i = 0; i < colCount; i++) {
			switch (colType[i]) {
			case MysqlDefs.FIELD_TYPE_BLOB:
				// fall through
			case MysqlDefs.FIELD_TYPE_GEOMETRY:
			case MysqlDefs.FIELD_TYPE_FLOAT:
				// fall through
			case MysqlDefs.FIELD_TYPE_DOUBLE:
				metadata[i] = result.readByte();
				j += 1;
				break;
			case MysqlDefs.FIELD_TYPE_BIT:
				// fall through
			case MysqlDefs.FIELD_TYPE_NEW_DECIMAL:
				// fall through
			case MysqlDefs.FIELD_TYPE_VARCHAR:
				// fall through
			case MysqlDefs.FIELD_TYPE_VAR_STRING:
				// fall through
			case MysqlDefs.FIELD_TYPE_STRING:
				metadata[i] = result.readInt();
				j += 2;
			default:
				break;
			}
		}

		if (j != metadataLen) {
			log.error("parse metadata length error: " + j + " vs "
					+ metadataLen);
		}

		final int nullBitsLen = (colCount + 7) / 8;
		final byte[] nullBits = new byte[nullBitsLen];
		for (int i = 0; i < nullBitsLen; ++i) {
			nullBits[i] = result.readByte();
		}

		if (log.isDebugEnabled()) {
			log.debug("tableMap: " + database + "." + table + " ==> " + tId
					+ " colCount:" + colCount + " metalen:" + metadataLen);
			for (int i = 0; i < colCount; ++i) {
				log.debug("col: " + i + " type: "
						+ MysqlDefs.typeToName(colType[i]) + " meta: "
						+ metadata[i]);
			}
		}

		tmap.setDatabase(database);
		tmap.setTable(table);
		tmap.setColCount(colCount);
		tmap.setColType(colType);
		tmap.setMetadataLen(metadataLen);
		tmap.setMetadata(metadata);
		tmap.setNullBits(nullBits);
	}

	/**
	 * @see mysqlbinlog.cc#dump_remote_log_entries
	 * @return
	 * @throws SQLException
	 */
	private Buffer makeDumpBinlogCommand(final String binlogFilename,
			final long binlogStartPos) throws SQLException {
		final Buffer command = localBuffer.get();
		command.clear();
		command.writeByte((byte) MysqlDefs.COM_BINLOG_DUMP);

		// int4store pos
		command.writeLong(binlogStartPos);

		// int2store binlog_flags=0
		command.writeInt(0);

		// int4store server id
		command.writeLong(slaveId);

		// memcpy(buf + 10, logname, logname_len);
		// XXX binlogFilename should be ascii ?
		command.writeString(binlogFilename);

		if (log.isDebugEnabled()) {
			log.debug("cmd: \n"
					+ StringUtils.dumpAsHex(command.getByteBuffer(),
							command.getPosition()));
		}

		return command;
	}

	private String readNullTerminatedString(final Buffer buf) {
		final int len = buf.readByte();
		final String result = new String(buf.getBytes(len));
		// terminated null
		buf.readByte();
		return result;
	}

	private static boolean bitMapIsSet(final byte[] bitmap, final int index) {
		if (index >= bitmap.length * 8) {
			log.warn("index out of scope: " + index);
			return false;
		}

		final int tidx = index / 8;
		final int fidx = index % 8;
		final byte bits = bitmap[tidx];
		final int value = (0x01 << fidx & 0xff) & bits;

		return value != 0;
	}

	public void debugBuffer(final Buffer result) {
		if (log.isDebugEnabled()) {
			final int start = result.getPosition();
			final int len = result.getBufLength() - start;
			log.debug("buffer len:" + len + " \n"
					+ StringUtils.dumpAsHex(result.getBytes(start, len), len));
		}
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(final String[] args) throws Exception {
		Parser.main(args);
	}
}
