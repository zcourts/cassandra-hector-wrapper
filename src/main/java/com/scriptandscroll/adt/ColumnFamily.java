package com.scriptandscroll.adt;

import com.scriptandscroll.exceptions.InvalidValueException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import static me.prettyprint.hector.api.factory.HFactory.*;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import me.prettyprint.hector.api.query.SliceQuery;

/**
 *
 * @author Courtney
 */
public class ColumnFamily {

	private Keyspace keyspace;
	private String name;
	private StringSerializer se;
	private Mutator<String> mutator;

	/**
	 * 
	 * @param ks
	 * @param name column family name
	 */
	public ColumnFamily(Keyspace ks, String name) {
		keyspace = ks;
		this.name = name;
		se = keyspace.getSerializer();
		mutator = createMutator(keyspace.getHectorKeyspace(), keyspace.getSerializer());
	}

	/**
	 * Wrapper for getColumns to simple return a Row object instead of a list of columns
	 * @param key the row key
	 * @param columnNames the set of columns to get
	 * @return A Row object containing all the columns returned form the query
	 */
	public Row getRow(String key, String[] columnNames) {
		return new Row(getColumns(key, columnNames));
	}

	/**
	 * Wrapper for getColumns
	 * @param key the row key
	 * @param start the start column name
	 * @param end the end column name
	 * @return 
	 */
	public Row getRow(String key, String start, String end) {
		return new Row(getColumns(key, start, end));
	}

	/**
	 * Wrapper for get columns
	 * @param key the row key
	 * @param start the start column name
	 * @param end the end column name
	 * @param reversed reversed or now
	 * @return 
	 */
	public Row getRow(String key, String start, String end, boolean reversed) {
		return new Row(getColumns(key, start, end, reversed));
	}

	/**
	 * Wrapper for getcolumns
	 * @param key the row key 
	 * @param start the start column
	 * @param end the end column
	 * @param reversed reversed or now
	 * @param count how many columns to limit the row to
	 * @return 
	 */
	public Row getRow(String key, String start, String end, boolean reversed, int count) {
		return new Row(getColumns(key, start, end, reversed, count));
	}

	/**
	 * Get  up to 10 columns from the row with the provided key in NONE REVERSED  order
	 * i.e. order is not reversed.
	 * @param key row key
	 * @param start start column name
	 * @param end end column name
	 * @return list of columns retrieved
	 */
	public List<Column> getColumns(String key, String start, String end) {
		return getColumns(key, start, end, false, 10);
	}

	/**
	 * Get requested columns in the order requested
	 * @param key the  row key
	 * @param start start column
	 * @param end end column
	 * @param reversed whether columns are reversed or not
	 * @return list of columns
	 */
	public List<Column> getColumns(String key, String start, String end, boolean reversed) {
		return getColumns(key, start, end, reversed, 10);
	}

	/**
	 * Get a list of columns 
	 * @param key the row key
	 * @param start the start column name
	 * @param end the end column name
	 * @param reversed reversed or not
	 * @param count how many columns to get
	 * @return a list of columns retrieved
	 */
	public List<Column> getColumns(String key, String start, String end, boolean reversed, int count) {
		SliceQuery<String, String, String> q = createSliceQuery(keyspace.getHectorKeyspace(), se, se, se);
		q.setColumnFamily(getName());
		q.setKey(key);
		q.setRange(start, end, reversed, count);
		QueryResult<ColumnSlice<String, String>> r = q.execute();
		ColumnSlice<String, String> slice = r.get();
		List<HColumn<String, String>> columns = slice.getColumns();
		List<Column> ret = new ArrayList<Column>();
		for (HColumn<String, String> c : columns) {
			ret.add(new Column(c));
		}
		return ret;
	}

	/**
	 * Get and return a set of columns defined by columnNames
	 * @param key the row ket
	 * @param columnNames the list of columns to get within the given row
	 * @return 
	 */
	public List<Column> getColumns(String key, String[] columnNames) {
		SliceQuery<String, String, String> q = createSliceQuery(keyspace.getHectorKeyspace(), se, se, se);
		q.setColumnFamily(getName());
		q.setKey(key);
		// try with column name first
		q.setColumnNames(columnNames);
		QueryResult<ColumnSlice<String, String>> r = q.execute();
		ColumnSlice<String, String> slice = r.get();
		List<HColumn<String, String>> columns = slice.getColumns();
		List<Column> ret = new ArrayList<Column>();
		for (HColumn<String, String> c : columns) {
			ret.add(new Column(c));
		}
		return ret;
	}

	/**
	 * Get a single column from a row
	 * @param key the row key to fetch the column from
	 * @param columnName the name of the column to fetch
	 * @return A Column containing the value of the requested column
	 */
	public Column getColumn(String key, String columnName) {
		ColumnQuery<String, String, String> q = createColumnQuery(keyspace.getHectorKeyspace(), se, se, se);
		q.setName(columnName).setColumnFamily(getName());
		QueryResult<HColumn<String, String>> r = q.setKey(key).execute();
		return new Column(r.get());
	}

	/**
	 * Deletes an entire row from cassandra
	 * @param rowKey removes the row with the given key, i.e. all columns!
	 * There's no going back to be sure!!!
	 */
	public void removeRow(String rowKey) {
		mutator.addDeletion(rowKey, getName(), null, se);
		mutator.execute();
	}

	/**
	 * Remove the given column
	 * @param key the row key
	 * @param col the col
	 * @throws InvalidValueException 
	 */
	public void removeColumn(String key, Column col) throws InvalidValueException {
		removeColumns(new Row(key, col));
	}

	/**
	 * Remove the column with the given name
	 * @param key row key
	 * @param colName
	 * @throws InvalidValueException if column name is null or empty
	 */
	public void removeColumn(String key, String colName) throws InvalidValueException {
		removeColumns(new Row(key, new Column(colName)));
	}

	/**
	 * Remove one or more columns from this row
	 * @param row the row from which columns are to be removed
	 */
	public void removeColumns(Row row) {
		if (row.isChanged()) {
			List<Column> cols = row.getRemovedColumns();
			if (!cols.isEmpty()) {
				for (Column col : cols) {
					mutator.addDeletion(row.getKey(), getName(), col.getName(), se);
				}
			}
			mutator.execute();
		}
	}

	/**
	 * Insert/Update row in the column family and automatically delete (from Cassandra) any columns
	 * "removed" from the row provided
	 * @param row row to insert into
	 */
	public void putRow(Row row) {
		putRow(row, true);
	}

	/**
	 * @param row the row to insert
	 * @param autoRemove if true then any columns that have been removed from the row object will be removed
	 * from Cassandra
	 */
	public void putRow(Row row, boolean autoRemove) {
		if (row.isChanged()) {
			Iterator<Column> it = row.iterator();
			while (it.hasNext()) {
				Column col = it.next();
				mutator.addInsertion(row.getKey(), getName(), createColumn(col.getName(), col.getValue(), se, se));
			}
			mutator.execute();
			if (autoRemove) {
				//auto remove any changed row
				removeColumns(row);
			}
		}
	}

	/**
	 * Add update the given column
	 * @param key the row key
	 * @param col the column to add or update
	 */
	public void putColumn(String key, Column col) {
		putColumn(key, col.getName(), col.getValue());
	}

	/**
	 * Add/Update a column in the given row
	 * @param key the row key
	 * @param colName the column name
	 * @param colVal the column value
	 */
	public void putColumn(String key, String colName, String colVal) {
		mutator.addInsertion(key, getName(), createColumn(colName, colVal, se, se));
		mutator.execute();
	}

	public String getName() {
		return name;
	}

	/**
	 * 
	 * Get multiple rows starting from the start key to the end key provided
	 * @param start the start key
	 * @param end the row key to end on
	 * @param startColumn
	 * @param endColumn
	 * @param reversed
	 * @param colCount max columns to return
	 * @param rowCount Max rows to return
	 * @return A list of row objects
	 */
	public List<Row> getRows(String start, String end, String startColumn, String endColumn, boolean reversed, int rowCount, int colCount) {
		List<Row> ret = new ArrayList<Row>();
		RangeSlicesQuery<String, String, String> q = createRangeSlicesQuery(keyspace.getHectorKeyspace(), se,
				se, se);
		q.setColumnFamily(getName());
		q.setKeys(start, end);
		q.setRowCount(rowCount);
		q.setRange(startColumn, endColumn, reversed, colCount);
		QueryResult<OrderedRows<String, String, String>> r = q.execute();
		OrderedRows<String, String, String> orderedrows = r.get();
		List<me.prettyprint.hector.api.beans.Row<String, String, String>> rows = orderedrows.getList();
		for (me.prettyprint.hector.api.beans.Row hectorRow : rows) {
			ColumnSlice<String, String> slice = hectorRow.getColumnSlice();
			List<HColumn<String, String>> columns = slice.getColumns();
			try {
				Row aRow = new Row((String) hectorRow.getKey());
				aRow.putHectorColumns(columns);
				ret.add(aRow);
			} catch (InvalidValueException ex) {
				Logger.getLogger(ColumnFamily.class.getName()).log(Level.SEVERE, null, ex);
			}
		}
		return ret;
	}

	/**
	 * Get up to 10 rows and 10 columns by default
	 * @param start
	 * @param end
	 * @param startColumn
	 * @param endColumn
	 * @param reversed
	 * @return 
	 */
	public List<Row> getRows(String start, String end, String startColumn, String endColumn, boolean reversed) {
		return getRows(start, end, startColumn, endColumn, reversed, 10, 10);
	}

	/**
	 * Get 10 rows and 10 columns by default in NONE REVERSED order
	 * @param start
	 * @param end
	 * @param startColumn
	 * @param endColumn
	 * @return 
	 */
	public List<Row> getRows(String start, String end, String startColumn, String endColumn) {
		return getRows(start, end, startColumn, endColumn, false, 10, 10);
	}

	/**
	 * Get a list of rows by specifying all the columns to be returned
	 * @param start start key
	 * @param end end key
	 * @param columnNames array of columns to be returned
	 * @param reversed
	 * @param count
	 * @return 
	 */
	public List<Row> getRows(String start, String end, String[] columnNames, boolean reversed, int count) {
		List<Row> ret = new ArrayList<Row>();
		RangeSlicesQuery<String, String, String> q = createRangeSlicesQuery(keyspace.getHectorKeyspace(), se,
				se, se);
		q.setColumnFamily(getName());
		q.setKeys(start, end);
		q.setColumnNames(columnNames);
		QueryResult<OrderedRows<String, String, String>> r = q.execute();
		OrderedRows<String, String, String> orderedrows = r.get();
		List<me.prettyprint.hector.api.beans.Row<String, String, String>> rows = orderedrows.getList();
		for (me.prettyprint.hector.api.beans.Row hectorRow : rows) {
			ColumnSlice<String, String> slice = hectorRow.getColumnSlice();
			List<HColumn<String, String>> columns = slice.getColumns();
			try {
				Row aRow = new Row((String) hectorRow.getKey());
				aRow.putHectorColumns(columns);
				ret.add(aRow);
			} catch (InvalidValueException ex) {
				Logger.getLogger(ColumnFamily.class.getName()).log(Level.SEVERE, null, ex);
			}
		}
		return ret;
	}

	/**
	 * Get 10 rows
	 * @param start start key
	 * @param end end key
	 * @param columnNames array of column names
	 * @param reversed 
	 * @return 
	 */
	public List<Row> getRows(String start, String end, String[] columnNames, boolean reversed) {
		return getRows(start, end, columnNames, reversed, 10);
	}

	/**
	 * Get a 10 rows in NONE REVERSED order
	 * @param start start key
	 * @param end end key
	 * @param columnNames array of column names
	 * @return 
	 */
	public List<Row> getRows(String start, String end, String[] columnNames) {
		return getRows(start, end, columnNames, false, 10);
	}
}
