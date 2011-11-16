package com.scriptandscroll.adt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 *
 * @author Courtney
 */
public abstract class SuperColumnContainer {

	private List<SuperColumn> removedSuperColumns;
	private HashMap<String, SuperColumn> columns;
	private boolean isChanged;

	public SuperColumnContainer() {
		columns = new HashMap<String, SuperColumn>();
		removedSuperColumns = new ArrayList<SuperColumn>();
	}

	/**
	 * Removes a SuperColumn from this container
	 * @param col  the SuperColumn to remove
	 */
	public void removeSuperColumn(SuperColumn col) {
		this.columns.remove(col.getName());
		removedSuperColumns.add(col);
		setChanged(true);
	}

	/**
	 * Get a list of columns removed
	 * @return 
	 */
	public List<SuperColumn> getRemovedSuperColumns() {
		return removedSuperColumns;
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
	public List<SuperColumn> getAllSuperColumns() {
		List<SuperColumn> all = new ArrayList<SuperColumn>();
		all.addAll(columns.values());
		return all;
	}

	/**
	 * @param name The name of the SuperColumn to return
	 * @return The SuperColumn or null if not found
	 */
	public SuperColumn getSuperColumn(String name) {
		return columns.get(name);
	}

	/**
	 * @param col Adds the given SuperColumn to this container.
	 */
	public void putSuperColumn(SuperColumn col) {
		this.columns.put(col.getName(), col);
		setChanged(true);
	}

	/**
	 * @param cols Adds the given list of columns to this container.
	 */
	public void putSuperColumns(List<SuperColumn> cols) {
		for (SuperColumn col : cols) {
			this.columns.put(col.getName(), col);
		}
		setChanged(true);
	}

	/**
	 * 
	 * @return true if this super column has no columns, false otherwise
	 */
	public boolean isEmpty() {
		return columns.isEmpty();
	}
}
