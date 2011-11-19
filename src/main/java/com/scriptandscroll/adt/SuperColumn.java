package com.scriptandscroll.adt;

import com.scriptandscroll.exceptions.InvalidValueException;
import java.util.ArrayList;
import java.util.List;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HSuperColumn;

/**
 *
 * @author Courtney
 */
public class SuperColumn<SN, N, V> extends ColumnContainer implements Savable {

	private String name;
	private Serializer namese;
	private Serializer supernamese;
	private Serializer valuese;

	public SuperColumn() {
		supernamese = new StringSerializer();
		namese = new StringSerializer();
		valuese = new StringSerializer();
	}

	/**
	 * Create  a super column with no sub-columns
	 * @param name the name of the super column 
	 * @throws InvalidValueException if name is empty or null
	 */
	public SuperColumn(String name) {
		this(name, new ArrayList<Column>());
	}

	public SuperColumn(HSuperColumn<SN, N, V> col) {
		this();
		this.name = (String) col.getName();
		fromHectorList(col.getColumns());
	}

	public SuperColumn(SN name, List<HColumn<N, V>> cols) {
		this();
		this.name = (String) name;
		fromHectorList(cols);
	}

	private void fromHectorList(List<HColumn<N, V>> cols) {
		for (HColumn<N, V> c : cols) {
			putColumn(new Column(c));
		}
	}

	/**
	 * Create a super column with a list of sub columns
	 * @param name the name of the super column within  the row
	 * @param columns a list of sub columns to add
	 * @throws InvalidValueException  if name is empty or null
	 */
	public SuperColumn(String name, ArrayList<Column> columns) {
		this();
		if (name == null || name.isEmpty()) {
			throw new InvalidValueException("The name of a column cannot be null or empty!");
		}
		this.name = name;
		for (Column col : columns) {
			putColumn(col);
		}
	}

	/**
	 * 
	 * @return Get the name of this Super column
	 */
	public String getName() {
		return name;
	}

	/**
	 * The serializer to be used on sub column names
	 * @param s 
	 */
	public final void setNameSerializer(Serializer s) {
		namese = s;
	}

	/**
	 * Set the serializer for the super column names
	 * @param s 
	 */
	public final void setSuperNameSerializer(Serializer s) {
		supernamese = s;
	}

	/**
	 * The serializer to be used on the sub column's values
	 * @param s 
	 */
	public final void setValueSerializer(Serializer s) {
		valuese = s;
	}

	/**
	 * The serializer to be used on this column's name
	 */
	public Serializer getNameSerializer() {
		return namese;
	}

	/**
	 * The serializer to be used on this column's value
	 */
	public Serializer getValueSerializer() {
		return valuese;
	}

	/**
	 * get the serializer for the super column names
	 * @param s 
	 */
	public final Serializer getSuperNameSerializer() {
		return supernamese;
	}
}
