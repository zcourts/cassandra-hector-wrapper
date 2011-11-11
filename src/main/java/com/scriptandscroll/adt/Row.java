package com.scriptandscroll.adt;

import com.scriptandscroll.exceptions.InvalidValueException;
import java.util.List;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;

/**
 *
 * @author Courtney
 */
public class Row extends ColumnContainer implements Savable {

	private String key;

	public Row(List<Column> columns) {
	}

	public Row(me.prettyprint.hector.api.beans.Row<String, String, String> row) {
		this.key = row.getKey();
		ColumnSlice<String, String> slice = row.getColumnSlice();
		List<HColumn<String, String>> columns = slice.getColumns();
		for (HColumn<String, String> col : columns) {
			putColumn(new Column(col));
		}
	}

	/**
	 * Create an empty row to which columns can be added later
	 * @param key the row key
	 * @throws InvalidValueException if key is null or empty
	 */
	public Row(String key) throws InvalidValueException {
		if (key == null || key.isEmpty()) {
			throw new InvalidValueException("The key of a column cannot be null or empty!");
		}
		this.key = key;
	}

	/**
	 * Create a row with a single columns
	 * @param key
	 * @param col
	 * @throws InvalidValueException if key is null or empty
	 */
	public Row(String key, Column col) throws InvalidValueException {
		if (key == null || key.isEmpty()) {
			throw new InvalidValueException("The key of a column cannot be null or empty!");
		}
		this.key = key;
		putColumn(col);
	}

	/**
	 * 
	 * @param key
	 * @param cols
	 * @throws InvalidValueException if key is null or empty
	 */
	public Row(String key, List<Column> cols) throws InvalidValueException {
		if (key == null || key.isEmpty()) {
			throw new InvalidValueException("The key of a column cannot be null or empty!");
		}
		this.key = key;
		for (Column col : cols) {
			putColumn(col);
		}
	}

	/**
	 * @return The row key this row object represents
	 */
	public String getKey() {
		return key;
	}
}
