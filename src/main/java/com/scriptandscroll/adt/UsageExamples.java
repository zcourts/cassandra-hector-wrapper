package com.scriptandscroll.adt;

import java.util.ArrayList;
import java.util.List;

/**
 *Shows basic usage of the classes.
 * It firstly makes no provision to allow you to create keyspaces or column families, YET!
 * But once those are created from the CLI or some other way it provides a way to deal with 
 * just about everything
 * @author Courtney Robinson <courtney@crlog.info>
 */
public class UsageExamples {

	public static void main(String[] args) {
		Keyspace ks = new Keyspace("clusterName", "keyspaceName", "localhost:9160");
		//Standard column family examples
		//create a column family object - THIS DOES NOT CREATE A COLUMN FAMILY IN CASSANDRA but assumes one with the given name already exists!
		ColumnFamily cf = new ColumnFamily(ks, "cfName");
		//now we can perform actions on this column family.
		//
		//first lets get a single column
		Column col = cf.getColumn("rowkey1", "columnName");
		//now we can use its value or name using
		col.getName();//returns a string
		//or 
		col.getValue();//returns a string
		//
		//
		//we can get a set of columns from a row in three ways, by giving a startand end column name
		List<Column> cols = cf.getColumns("rowkey2", "startCol", "endCol");
		//by giving start and end col names and specifying a max amount of cols to get 
		List<Column> cols2 = cf.getColumns("rowke2", "startCol", "endCol", false, 5);
		//or by giving an array of all columns to get
		//in this case it will only return the given columns
		List<Column> cols3 = cf.getColumns("rowkey2", new String[]{"col1", "col2", "col3", "col4"});
		//
		//We can also get rows within a CF
		//by setting start and end column names to an empty string and not setting a max value
		//we can get all the columns within the given row
		//the same options as getColumns apply, you specify columns by start and end key with an optional max amount or an array of columns
		Row row = cf.getRow("rowkey", "", "");
		//you can now do cool stuff with this row object like add and remove columns.
		//if you later pass this object to a column family it will apply those changes in Cassandra e.g.
		row.putColumn("newColName", "newColValue");
		//or
		row.putColumn(new Column("newerColName", "newerColValue"));
		//while we're at it we can remove columns from this row
		row.removeColumn(col);
		//or
		row.removeColumn("colName");
		//if we now write this row back to the column family all those changes are applied
		cf.putRow(row);//that's it! two new columns will be added, and two removed
		//we coould do
		//setting false stops it removing columns from cassandra that were removed from the object
		//columns that were added are still added obviously...
		cf.putRow(row, false);
		//we can also get multiple rows like this
		//setting start and end row keys and column names to empty gets everything
		//but we set the max rows to return as 20 and the max columns per row to 5
		//so up to 20 rows are returned which will contain up to 5 columns
		//there are multiple variations on these methods that allows various operations
		List<Row> rows = cf.getRows("", "", "", "", false, 20, 5);
		//Simple? Good! That is the aim!

		//Lukily Super column family operations work in a similar manner
		SuperColumnFamily scf = new SuperColumnFamily(ks, "superCFName");
		//now go through the same thing again...
		SuperColumn scol = scf.getSuperColumn("rowkey", "supercolName");
		//get sub columns of this super column
		List<Column> subCols = scol.getAllColumns();
		//or get multiple super columns
		List<SuperColumn> scol2 = scf.getSuperColumns("rowkey", new String[]{"superCol1", "superCol2", "superCol3"});

		//get a single sub column
		Column subCol = scf.getSubColumn("rowket", "superColname", "subcolname");
		List<Column> subCols2 = scf.getSubColumns("rowkey", "superCol", "startSubcol", "endSubCol");

		//we can get sub columns from multiple rows
		List<String> keys = new ArrayList<String>();
		keys.add("key1");
		keys.add("key2");
		keys.add("key3");
		keys.add("key4");
		keys.add("key5");
		//gets a list of rows with the sub columns requested
		List<Row> rowSubCols = scf.getSubColumnsFromMultipleRows(keys, "superColumn", "startSubCol", "endSubCol", false, 20);
		//get an entire super row
		SuperRow srow = scf.getSuperRow("rowkey", "startColumn", "endCol");
		SuperColumn sc = srow.getSuperColumn("superCol");//now do what we want
		List<SuperRow> lsuperRows = scf.getSuperRows(keys, "startCol", "endCol");
		//get up to 20 rows
		List<SuperRow> srows2 = scf.getSuperRows("startKey", "endKey", new String[]{}, 20);

		//we can also add and remove from a super row just as we did with a normal row
		ArrayList<Column> cols5 = new ArrayList();
		cols5.add(new Column("subname", "subval"));

		srow.putSuperColumn(new SuperColumn("colname", cols5));
		//and now
		scf.putSuperRow(srow);
		//all done...
		//still simple? 
	}
}
