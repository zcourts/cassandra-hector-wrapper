package com.scriptandscroll.adt;

import com.scriptandscroll.exceptions.InvalidValueException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import me.prettyprint.hector.api.beans.HColumn;

/**
 *
 * @author Courtney
 */
public abstract class ColumnContainer {

	private List<Column> removedColumns;
	private HashMap<String, Column> columns;
	private boolean isChanged;

	public ColumnContainer() {
		columns = new HashMap<String, Column>();
		removedColumns = new ArrayList<Column>();
	}

	/**
	 * Removes a column from this container
	 * @param col  the column to remove
	 */
	public void removeColumn(Column col) {
		this.columns.remove(col.getName());
		removedColumns.add(col);
		setChanged(true);
	}

	/**
	 * Get a list of columns removed
	 * @return 
	 */
	public List<Column> getRemovedColumns() {
		return removedColumns;
	}

	/**
	 * Returns an iterator over the columns in this container. 
	 * There are no guarantees concerning the order in which the elements are returned
	 * @return an Iterator over the columns in this container
	 */
	public Iterator iterator() {
		return columns.values().iterator();
	}

	/**
	 * @return true if any property of this object has been modified
	 */
	public boolean isChanged() {
		return isChanged;
	}

	/**
	 * Mark this object as being changed or unchanged (modified/unmodified)
	 * @param changed If true then all changes will be automatically committed to
	 * Cassandra, If false then even if changes were made they will not be written to Cassandra
	 */
	public void setChanged(boolean changed) {
		isChanged = changed;
	}

	/**
	 * @return A list of all the columns this container currently has.
	 * In reality if the query that resulted in this container object limited the number of columns
	 * then the amount of columns returned will be up to that limit, i.e.
	 * all columns means all columns returned by the query which is not necessarily all the columns
	 * stored in Cassandra
	 */
	public List<Column> getAllColumns() {
		List<Column> all = new ArrayList<Column>();
		all.addAll(columns.values());
		return all;
	}

	/**
	 * @param name The name of the column to return
	 * @return The column or null if not found
	 */
	public Column getColumn(String name) {
		return columns.get(name);
	}

	/**
	 * Get the Value of a column
	 * @param name the name of the column to get the value from
	 * @return the String alue of the given column
	 */
	public String getColumnValue(String name) {
		return columns.get(name).getValue();
	}

	/**
	 * @param col Adds the given column to this container.
	 */
	public void putColumn(Column col) {
		this.columns.put(col.getName(), col);
		setChanged(true);
	}

	/**
	 * @param cols Adds the given list of columns to this container.
	 */
	public void putColumns(List<Column> cols) {
		for (Column col : cols) {
			this.columns.put(col.getName(), col);
		}
		setChanged(true);
	}

	/**
	 * Adds a new sub column to this container
	 * @param colname the name of the column
	 * @param value the column's value
	 * @throws InvalidValueException 
	 */
	public void putColumn(String colname, String value) throws InvalidValueException {
		putColumn(new Column(colname, value));
	}

	/**
	 * Add a list of HColumns to this row
	 * @param columns the list
	 */
	public void putHectorColumns(List<HColumn<String, String>> columns) {
		for (HColumn col : columns) {
			this.columns.put((String) col.getName(), new Column(col));
		}
		setChanged(true);
	}
}
