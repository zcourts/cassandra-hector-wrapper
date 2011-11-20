package com.scriptandscroll.adt;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

/**
 *Allows building a query to run against an indexed column or a set of indexed columns
 * @author Courtney Robinson <courtney@crlog.info>
 */
public class IndexQueryBuilder {

	List<Column> equals;
	List<Column> lessthan;
	List<Column> greaterthan;

	public IndexQueryBuilder() {
		equals = new ArrayList<Column>();
		lessthan = new ArrayList<Column>();
		greaterthan = new ArrayList<Column>();
	}

	/**
	 * Adds an equal comparison to be performed
	 * @param column the name of the indexed column to perform the equals match against
	 * @param value the value to compare with
	 */
	public <N, V> void addEqualsComparison(N column, V value) {
		equals.add(new Column(column, value));
	}

	/**
	 * Adds a less than comparison to be performed
	 * @param column the name of the indexed column to perform the less than match against
	 * @param value the value to compare with
	 */
	public <N, V> void addLessThanComparison(N column, V value) {
		lessthan.add(new Column(column, value));
	}

	/**
	 * Adds a greater than comparison to be performed
	 * @param column the name of the indexed column to perform the greater than match against
	 * @param value the value to compare with
	 */
	public <N, V> void addGreaterThanComparison(N column, V value) {
		greaterthan.add(new Column(column, value));
	}

	public ListIterator<Column> equalsIterator() {
		return equals.listIterator();
	}

	public ListIterator<Column> lessThanIterator() {
		return lessthan.listIterator();
	}

	public ListIterator<Column> greaterThanIterator() {
		return greaterthan.listIterator();
	}
}
