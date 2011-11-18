package com.scriptandscroll.adt;

import me.prettyprint.hector.api.Serializer;
import java.util.Iterator;
import me.prettyprint.hector.api.beans.SuperRows;
import me.prettyprint.hector.api.query.MultigetSuperSliceQuery;
import java.util.Collection;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.query.MultigetSubSliceQuery;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.query.SubSliceQuery;
import me.prettyprint.hector.api.beans.OrderedSuperRows;
import me.prettyprint.hector.api.query.RangeSuperSlicesQuery;
import com.scriptandscroll.exceptions.InvalidValueException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import me.prettyprint.hector.api.beans.SuperSlice;
import me.prettyprint.hector.api.query.SuperSliceQuery;
import me.prettyprint.hector.api.query.SubColumnQuery;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HSuperColumn;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
//
import me.prettyprint.hector.api.query.SuperColumnQuery;
import static me.prettyprint.hector.api.factory.HFactory.*;

/**
 *
 * @author Courtney
 */
public class SuperColumnFamily {

	private Keyspace keyspace;
	private String name;
	private Serializer se;
	private Mutator<String> mutator;

	/**
	 * 
	 * @param ks
	 * @param name column family name
	 */
	public SuperColumnFamily(Keyspace ks, String name) {
		keyspace = ks;
		this.name = name;
		se = new StringSerializer();
		mutator = createMutator(keyspace.getHectorKeyspace(), se);
	}

	public SuperColumnFamily(Keyspace ks, String name, Serializer s) {
		keyspace = ks;
		this.name = name;
		se = s;
		mutator = createMutator(keyspace.getHectorKeyspace(), s);
	}

	/**
	 * Get a single super column and all its sub columns
	 * @param row the row key
	 * @param column the name of the super column
	 * @return A super column object with all the column's sub columns
	 */
	public SuperColumn getSuperColumn(String row, String column) {
		SuperColumnQuery<String, String, String, String> q = createSuperColumnQuery(
				keyspace.getHectorKeyspace(), se, se, se, se);
		q.setSuperName(column).setColumnFamily(getName());
		QueryResult<HSuperColumn<String, String, String>> r = q.setKey(row).execute();
		HSuperColumn<String, String, String> sc = r.get();
		return new SuperColumn(column, sc.getColumns());
	}

	/**
	 * Get a single sub column form the given super column
	 * @param row the row key
	 * @param column the super column name
	 * @param subColumn the sub column name
	 * @return the sub column requested
	 */
	public Column getSubColumn(String row, String column, String subColumn) {
		SubColumnQuery<String, String, String, String> q =
				createSubColumnQuery(keyspace.getHectorKeyspace(), se, se, se, se);
		q.setSuperColumn(column).setColumn(subColumn).setColumnFamily(getName());
		QueryResult<HColumn<String, String>> r = q.setKey(row).execute();
		HColumn<String, String> c = r.get();
		if (c == null) {
			return null;
		} else {
			return new Column(c);
		}
	}

	public List<SuperColumn> getSuperColumns(String row, String[] columns) {
		List<SuperColumn> ret = new ArrayList<SuperColumn>();
		SuperSliceQuery<String, String, String, String> q = createSuperSliceQuery(
				keyspace.getHectorKeyspace(), se, se, se, se);
		q.setColumnFamily(getName());
		q.setKey(row);
		q.setColumnNames(columns);
		QueryResult<SuperSlice<String, String, String>> r = q.execute();
		SuperSlice<String, String, String> slice = r.get();
		List<HSuperColumn<String, String, String>> cols = slice.getSuperColumns();
		for (HSuperColumn<String, String, String> col : cols) {
			ret.add(new SuperColumn(col));
		}
		return ret;
	}

	public List<SuperColumn> getSuperColumns(String row, String start, String end, boolean reversed, int count) {
		List<SuperColumn> ret = new ArrayList<SuperColumn>();
		SuperSliceQuery<String, String, String, String> q = createSuperSliceQuery(
				keyspace.getHectorKeyspace(), se, se, se, se);
		q.setColumnFamily(getName());
		q.setKey(row);
		q.setRange(start, end, reversed, count);
		QueryResult<SuperSlice<String, String, String>> r = q.execute();
		SuperSlice<String, String, String> slice = r.get();
		List<HSuperColumn<String, String, String>> cols = slice.getSuperColumns();
		for (HSuperColumn<String, String, String> col : cols) {
			ret.add(new SuperColumn(col));
		}
		return ret;
	}

	public SuperRow getSuperRow(String key, String[] columns) {
		try {
			return new SuperRow(key, getSuperColumns(key, columns));
		} catch (InvalidValueException ex) {
			Logger.getLogger(SuperColumnFamily.class.getName()).log(Level.SEVERE, null, ex);
			return null;
		}
	}

	public SuperRow getSuperRow(String row, String start, String end) {
		return getSuperRow(row, start, end, false, 10);
	}

	public SuperRow getSuperRow(String row, String start, String end, boolean reversed) {
		return getSuperRow(row, start, end, reversed, 10);
	}

	public SuperRow getSuperRow(String row, String start, String end, boolean reversed, int count) {
		try {
			return new SuperRow(row, getSuperColumns(row, start, end, reversed, count));
		} catch (InvalidValueException ex) {
			Logger.getLogger(SuperColumnFamily.class.getName()).log(Level.SEVERE, null, ex);
			return null;
		}
	}

	/**
	 * Get a set of super columns and all their sub columns
	 * If either either or both the startColumn and endColumn are empty strings then
	 * the number of columns returned are up to the amount specified by count
	 * @param startKey the start row key
	 * @param endKey the row key to stop at
	 * @param startColumn the column to start at
	 * @param endColumn the column to end at
	 * @param reversed 
	 * @param rowCount max rows to return
	 * @param colCount max columns to return
	 * @return 
	 */
	public List<SuperRow> getSuperRows(String startKey, String endKey, String startColumn, String endColumn, boolean reversed, int rowCount, int colCount) {
		List<SuperRow> ret = new ArrayList<SuperRow>();
		RangeSuperSlicesQuery<String, String, String, String> q = createRangeSuperSlicesQuery(
				keyspace.getHectorKeyspace(), se, se, se, se);
		q.setColumnFamily(getName());
		q.setKeys(startKey, endKey);
		q.setRowCount(rowCount);
		q.setRange(startColumn, endColumn, reversed, colCount);
		QueryResult<OrderedSuperRows<String, String, String, String>> r = q.execute();
		OrderedSuperRows<String, String, String, String> rows = r.get();
		List<me.prettyprint.hector.api.beans.SuperRow<String, String, String, String>> rowList = rows.getList();
		for (me.prettyprint.hector.api.beans.SuperRow<String, String, String, String> row : rowList) {
			ret.add(new SuperRow(row));
		}
		return ret;
	}

	/**
	 * Get a list of row objects for the keys given by the collection
	 * @param keys a collection of keys to fetch
	 * @param startKey the column key to start from
	 * @param endKey the column key to end with
	 * @return a list of row one for each key in the given collection
	 */
	public List<SuperRow> getSuperRows(Collection<String> keys, String startKey, String endKey) {
		// get value
		MultigetSuperSliceQuery<String, String, String, String> q = createMultigetSuperSliceQuery(
				keyspace.getHectorKeyspace(), se, se, se, se);
		q.setColumnFamily(getName());
		q.setKeys(keys);
		// try with column name first
		q.setColumnNames(startKey, endKey);
		QueryResult<SuperRows<String, String, String, String>> r = q.execute();
		SuperRows<String, String, String, String> rows = r.get();
		List<SuperRow> ret = new ArrayList<SuperRow>();
		for (me.prettyprint.hector.api.beans.SuperRow<String, String, String, String> row : rows) {
			ret.add(new SuperRow(row));
		}
		return ret;
	}

	/**
	 * Get a set of rows and their super columns as specified by the columns parameter
	 * @param startKey thestartrow key
	 * @param endKey the end row key
	 * @param columns a list of super columns to return
	 * @param rowCount max rows to return
	 * @return 
	 */
	public List<SuperRow> getSuperRows(String startKey, String endKey, String[] columns, int rowCount) {
		List<SuperRow> ret = new ArrayList<SuperRow>();
		RangeSuperSlicesQuery<String, String, String, String> q = createRangeSuperSlicesQuery(
				keyspace.getHectorKeyspace(), se, se, se, se);
		q.setColumnFamily(getName());
		q.setKeys(startKey, endKey);
		q.setColumnNames(columns);
		q.setRowCount(rowCount);
		QueryResult<OrderedSuperRows<String, String, String, String>> r = q.execute();
		OrderedSuperRows<String, String, String, String> rows = r.get();
		List<me.prettyprint.hector.api.beans.SuperRow<String, String, String, String>> rowList = rows.getList();
		for (me.prettyprint.hector.api.beans.SuperRow<String, String, String, String> row : rowList) {
			ret.add(new SuperRow(row));
		}
		return ret;
	}

	/**
	 * Get a sub slice from multiple rows. I.E. for each of the row keys provided,
	 * get a set of columns from the super column provided... In each row only get sub columns
	 * from super column, basically.
	 * @param keys the set of rows from which to get the sub columns
	 * @param superColumn the super column family from which the sub columns are retrieved 
	 * @param startSubColumn the start of the sub column slice
	 * @param endSubColumn the end of the sub column slice
	 * @param reversed
	 * @param count
	 * @return a list of rows containing all the sub columns
	 */
	public List<Row> getSubColumnsFromMultipleRows(Collection<String> keys, String superColumn, String startSubColumn, String endSubColumn, boolean reversed, int count) {
		MultigetSubSliceQuery<String, String, String, String> q = createMultigetSubSliceQuery(
				keyspace.getHectorKeyspace(), se, se, se, se);
		q.setColumnFamily(getName());
		q.setSuperColumn(superColumn);
		q.setKeys(keys);
		q.setColumnNames(endSubColumn, endSubColumn);
		QueryResult<Rows<String, String, String>> r = q.execute();
		Rows<String, String, String> rows = r.get();
		List<Row> ret = new ArrayList<Row>();
		for (String k : keys) {
			me.prettyprint.hector.api.beans.Row<String, String, String> row = rows.getByKey(k);
			ret.add(new Row(row));
		}
		return ret;
	}

	/**
	 * Get a set super columns from multiple rows
	 * @param keys the list or row keys to get columns from
	 * @param startColumn the start super column
	 * @param endColumn the end super column
	 * @param reversed
	 * @param count
	 * @return A list of super rows containing the requested super columns and their sub columns
	 */
	public List<SuperRow> getMultipleSuperSlices(Collection<String> keys, String startColumn, String endColumn, boolean reversed, int count) {
		MultigetSuperSliceQuery<String, String, String, String> q = createMultigetSuperSliceQuery(
				keyspace.getHectorKeyspace(), se, se, se, se);
		q.setColumnFamily(getName());
		q.setKeys(keys);
		// try with column name first
		q.setRange(startColumn, endColumn, reversed, count);
		QueryResult<SuperRows<String, String, String, String>> r = q.execute();
		SuperRows<String, String, String, String> rows = r.get();
		Iterator<me.prettyprint.hector.api.beans.SuperRow<String, String, String, String>> it = rows.iterator();
		List<SuperRow> ret = new ArrayList<SuperRow>();
		while (it.hasNext()) {
			me.prettyprint.hector.api.beans.SuperRow<String, String, String, String> row = it.next();
			ret.add(new SuperRow(row));
		}
		return ret;
	}

	public List<Column> getSubColumns(String key, String superColumn, String startSubColumn, String endSubColumn) {
		return getSubColumns(key, superColumn, startSubColumn, endSubColumn, false, 10);
	}

	public List<Column> getSubColumns(String key, String superColumn, String startSubColumn, String endSubColumn, boolean reversed) {
		return getSubColumns(key, superColumn, startSubColumn, endSubColumn, reversed, 10);
	}

	/**
	 * Get a range of sub columns within a given super column starting from startSubcolumn to endSubcolumn
	 * @param key the row key
	 * @param superColumn the name of the super column
	 * @param startSubColumn the sub column to start from
	 * @param endSubColumn the subcolumn to end at
	 * @param reversed
	 * @param count max sub columns to return
	 * @return 
	 */
	public List<Column> getSubColumns(String key, String superColumn, String startSubColumn, String endSubColumn, boolean reversed, int count) {
		return getSubColumns(key, superColumn, startSubColumn, endSubColumn, reversed, count, se, se, se, se);
	}

	/**
	 * Get a range of sub columns within a given super column starting from startSubcolumn to endSubcolumn
	 * @param key the row key
	 * @param superColumn the name of the super column
	 * @param startSubColumn the sub column to start from
	 * @param endSubColumn the subcolumn to end at
	 * @param reversed
	 * @param count max sub columns to return
	 * @param keySerializer The row key serializer
	 * @param sNameSerializer the super column name serializer
	 * @param nameSerializer the SUB column name serializer
	 * @param valueSerializer the SUB column value serializer
	 * @return 
	 */
	public List<Column> getSubColumns(String key, String superColumn, String startSubColumn, String endSubColumn, boolean reversed, int count, Serializer keySerializer,
			Serializer sNameSerializer, Serializer nameSerializer,
			Serializer valueSerializer) {
		SubSliceQuery<String, String, String, String> q = createSubSliceQuery(keyspace.getHectorKeyspace(),
				se, se, se, se);
		q.setColumnFamily(getName());
		q.setSuperColumn(superColumn);
		q.setKey(key);
		//sub column range
		q.setRange(startSubColumn, endSubColumn, reversed, count);
		QueryResult<ColumnSlice<String, String>> r = q.execute();
		ColumnSlice<String, String> slice = r.get();
		List<Column> ret = new ArrayList<Column>();
		for (HColumn<String, String> col : slice.getColumns()) {
			ret.add(new Column(col));
		}
		return ret;
	}

	public String getName() {
		return name;
	}

	/**
	 * Add the given row and its column and sub columns to this column family.
	 * Automatically remove any columns or sub columns marked for deletion in this row
	 * @param row the row to add
	 */
	public void putSuperRow(SuperRow row) {
		putSuperRow(row, true);
	}

	/**
	 * Save the given row to Cassandra
	 * @param row the row to write to Cas
	 * @param autoremove If true then any columns or sub columns "removed" from this row object
	 * is also removed from Cassandra
	 */
	public void putSuperRow(SuperRow row, boolean autoremove) {
		Mutator<String> m = createMutator(keyspace.getHectorKeyspace(), row.getKeySerializer());
		List<SuperColumn> supercols = row.getAllSuperColumns();
		for (SuperColumn superCol : supercols) {
			List<Column> mycols = superCol.getAllColumns();
			List<HColumn<String, String>> cols = new ArrayList<HColumn<String, String>>();
			for (Column col : mycols) {
				cols.add(createColumn(col.getName(), col.getValue(), col.getNameSerializer(), col.getValueSerializer()));
			}
			HSuperColumn<String, String, String> sc = createSuperColumn(superCol.getName(), cols, superCol.getSuperNameSerializer(), superCol.getNameSerializer(), superCol.getValueSerializer());
			m.addInsertion(row.getKey(), getName(), sc);
		}
		m.execute();
		if (autoremove) {
			removeSuperColumns(row);
			removeSubColumns(row);
		}
	}

	/**
	 * Remove any "deleted" super columns from this row
	 * @param row the row to remove super columns from
	 */
	public void removeSuperColumns(SuperRow row) {
		Mutator<String> m = createMutator(keyspace.getHectorKeyspace(), row.getKeySerializer());
		List<SuperColumn> deletedSupercolumns = row.getRemovedSuperColumns();
		for (SuperColumn superCol : deletedSupercolumns) {
			m.superDelete(row.getKey(), getName(), superCol.getName(), superCol.getSuperNameSerializer());
		}
		m.execute();
	}

	/**
	 * Remove all sub columns marked as deleted from the given row
	 * @param row the row from which to delete sub columns of the included super columns
	 */
	public void removeSubColumns(SuperRow row) {
		Mutator<String> m = createMutator(keyspace.getHectorKeyspace(), row.getKeySerializer());
		List<SuperColumn> deletedSupercolumns = row.getRemovedSuperColumns();
		for (SuperColumn superCol : deletedSupercolumns) {
			List<Column> mycols = superCol.getRemovedColumns();
			for (Column col : mycols) {
				m.subDelete(row.getKey(), getName(), superCol.getName(), col.getName(), superCol.getSuperNameSerializer(), col.getNameSerializer());
			}
		}
		m.execute();
	}
}
