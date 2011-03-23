/**
 * 
 */
package com.mysql.jdbc;

/**
 * @author tangfulin
 * 
 */
public class TableMap {

	private String database;
	private String table;
	private int colCount;
	private int[] colType;
	private int metadataLen;
	private int[] metadata;
	private byte[] nullBits;

	public String getDatabase() {
		return database;
	}

	public void setDatabase(final String database) {
		this.database = database;
	}

	public String getTable() {
		return table;
	}

	public void setTable(final String table) {
		this.table = table;
	}

	public int getColCount() {
		return colCount;
	}

	public void setColCount(final int colCount) {
		this.colCount = colCount;
	}

	public int[] getColType() {
		return colType;
	}

	public void setColType(final int[] colType) {
		this.colType = colType;
	}

	public int getMetadataLen() {
		return metadataLen;
	}

	public void setMetadataLen(final int metadataLen) {
		this.metadataLen = metadataLen;
	}

	public int[] getMetadata() {
		return metadata;
	}

	public void setMetadata(final int[] metadata) {
		this.metadata = metadata;
	}

	public byte[] getNullBits() {
		return nullBits;
	}

	public void setNullBits(final byte[] nullBits) {
		this.nullBits = nullBits;
	}

	/**
	 * @param args
	 */
	public static void main(final String[] args) {

	}

}
