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
public class SuperRow<K, SN, N, V> extends SuperColumnContainer implements Savable {

	private K key;
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
		this.key = (K) key;
	}

	public SuperRow(me.prettyprint.hector.api.beans.SuperRow<K, SN, N, V> row) {
		this();
		SuperSlice< SN, N, V> slice = row.getSuperSlice();
		List<HSuperColumn< SN, N, V>> cols = slice.getSuperColumns();
		for (HSuperColumn<SN, N, V> col : cols) {
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
	public SuperRow(K key, SuperColumn col) {
		this();
		if (key == null) {
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
	public SuperRow(K key, List<SuperColumn> cols) {
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
		return (String) key;
	}

	public void setKey(K key) {
		this.key = key;
	}

	public Serializer getKeySerializer() {
		return se;
	}
}
