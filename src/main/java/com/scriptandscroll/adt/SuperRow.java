package com.scriptandscroll.adt;

import com.scriptandscroll.exceptions.InvalidValueException;
import java.util.List;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.HSuperColumn;
import me.prettyprint.hector.api.beans.SuperSlice;

/**
 *
 * @author Courtney
 */
public class SuperRow extends SuperColumnContainer implements Savable {

	private String key;
	private Serializer se;

	private SuperRow() {
		se = new StringSerializer();
	}

	/**
	 * Create an empty row to which columns can be added later
	 * @param key the row key
	 * @throws InvalidValueException if key is null or empty
	 */
	public SuperRow(String key) {
		this();
		if (key == null || key.isEmpty()) {
			throw new InvalidValueException("The key of a column cannot be null or empty!");
		}
		this.key = key;
	}

	public SuperRow(me.prettyprint.hector.api.beans.SuperRow<String, String, String, String> row) {
		this();
		SuperSlice<String, String, String> slice = row.getSuperSlice();
		List<HSuperColumn<String, String, String>> cols = slice.getSuperColumns();
		for (HSuperColumn<String, String, String> col : cols) {
			putSuperColumn(new SuperColumn(col));
		}
		this.key = row.getKey();
	}

	/**
	 * Create a row with a single columns
	 * @param key
	 * @param col
	 * @throws InvalidValueException if key is null or empty
	 */
	public SuperRow(String key, SuperColumn col) {
		this();
		if (key == null || key.isEmpty()) {
			throw new InvalidValueException("The key of a column cannot be null or empty!");
		}
		this.key = key;
		putSuperColumn(col);
	}

	/**
	 * 
	 * @param key
	 * @param cols
	 * @throws InvalidValueException if key is null or empty
	 */
	public SuperRow(String key, List<SuperColumn> cols) {
		this();
		this.key = key;
		for (SuperColumn col : cols) {
			putSuperColumn(col);
		}
	}

	/**
	 * @return The row key this row object represents
	 */
	public String getKey() {
		return key;
	}

	public String setKey(String key) {
		return this.key = key;
	}

	public Serializer getKeySerializer() {
		return se;
	}
}
