package com.scriptandscroll.adt;

import com.scriptandscroll.exceptions.InvalidValueException;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.Date;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import me.prettyprint.cassandra.serializers.BooleanSerializer;
import me.prettyprint.cassandra.serializers.ByteBufferSerializer;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.DateSerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.HColumn;

/**
 *
 * @author Courtney
 */
public class Column<N, V> implements Savable {

	private boolean isChanged;
	private N name;
	private V value;
	private Serializer namese;
	private Serializer valuese;

	public Column() {
		namese = new StringSerializer();
		valuese = new StringSerializer();
	}

	/**
	 * Creates a Column from a Hector column
	 * @param hectorCol 
	 */
	public Column(HColumn<N, V> hectorCol) {
		this();
		if (hectorCol == null) {
			return;
		}
		name = hectorCol.getName();
		value = hectorCol.getValue();
		setNameSerializer(hectorCol.getNameSerializer());
		setValueSerializer(hectorCol.getValueSerializer());
	}

	/**
	 * Create a column who's value can be set later or if left unset 
	 * a column is created with an empty string as its value
	 * @param name the column name
	 * @throws  InvalidValueException When the name of the column is either null or empty
	 */
	public Column(String name) {
		this(name, null);
	}

	public Column(N name, V value) {
		this("" + name, "" + value);
		setNameSerializer(SerializerTypeInferer.getSerializer(name));
		setValueSerializer(SerializerTypeInferer.getSerializer(value));
	}

	/**
	 * Wraps Column(String name,String value) and sets the column's value
	 * serializer to the IntegerSerializer type
	 * @param name the column name
	 * @param value the int value of the column
	 */
	public Column(String name, int value) {
		this(name, "" + value);
		setValueSerializer(SerializerTypeInferer.getSerializer(value));
	}

	/**
	 * Wraps Column(String name,String value) and sets the column's value
	 * serializer to the LongSerializer type
	 * @param name the column name
	 * @param value the long value of the column
	 */
	public Column(String name, long value) {
		this(name, "" + value);
		setValueSerializer(SerializerTypeInferer.getSerializer(value));
	}

	/**
	 * Create a column without specifying a timestamp and allow one to be generated
	 * @param name the column name
	 * @param value the value for the column
	 * @throws  InvalidValueException When the name of the column is either null or empty
	 */
	public Column(String name, String value) {
		this();
		if (name == null || name.isEmpty()) {
			throw new InvalidValueException("The name of a column cannot be null or empty!");
		}
		this.name = (N) name;
		this.value = (V) value;
	}

	/**
	 * @return The name of this column
	 */
	public String getName() {
		return (String) name;
	}

	/**
	 * @return The value for this column
	 */
	public String getValue() {
		return (String) value;
	}

	public String getLongAsString() {
		return deserializeLong((Long) value);
	}

	/**
	 * Get the name of this column as the type determined by its serializer
	 * @return 
	 */
	public N getTypedName() {
		return name;
	}

	/**
	 * Get the value of this column as the type determined by its serializer
	 * @return 
	 */
	public V getTypedValue() {
		return value;
	}

	private String deserializeLong(long val) {
		try {
			ByteBuffer bf = LongSerializer.get().toByteBuffer(val);
			Charset charset = Charset.forName("ISO-8859-1");
			CharsetDecoder decoder = charset.newDecoder();
			return decoder.decode(bf).toString();
		} catch (CharacterCodingException ex) {
			Logger.getLogger(Column.class.getName()).log(Level.SEVERE, ex.getMessage());
			return "";
		}
	}

	/**
	 * Get the value of this column as a type determined by its serializer.
	 * For e.g. to get an int pass the IntegerSerializer
	 * @param <T> The type returned
	 * @param serializer the serializer to use to determine the type of the value 
	 * @return 
	 */
	public <T> T getValueAs(Class<T> serializer) {
		T type = null;
		Object[] params = {value};
		if (serializer.equals(UUIDSerializer.class) || serializer.equals(UUID.class)) {
			//nothing yet
		} else if (serializer.equals(StringSerializer.class) || serializer.equals(String.class)) {
			type = (T) value;
		} else if (serializer.equals(LongSerializer.class) || serializer.equals(Long.class)) {
			type = createObject(Long.class, params);
		} else if (serializer.equals(IntegerSerializer.class) || serializer.equals(Integer.class)) {
			type = createObject(Integer.class, params);
		} else if (serializer.equals(BooleanSerializer.class) || serializer.equals(Boolean.class)) {
			type = createObject(Boolean.class, params);
		} else if (serializer.equals(BytesArraySerializer.class) || serializer.equals(Byte[].class)) {
			type = createObject(Byte[].class, params);
		} else if (serializer.equals(ByteBufferSerializer.class) || serializer.equals(Byte.class)) {
			type = createObject(ByteBuffer.class, params);
		} else if (serializer.equals(DateSerializer.class) || serializer.equals(Date.class)) {
			type = createObject(Date.class, params);
		} else {
			type = createObject(Object.class, params);
		}
		return type;
	}

	/**
	 * Get the name of this column as a type determined by its serializer.
	 * For e.g. to get an int pass the IntegerSerializer
	 * @param <T> The type returned
	 * @param serializer the serializer to use to determine the type of the name 
	 * @return 
	 */
	public <T> T getNameAs(Class<T> serializer) {
		T type = null;
		Object[] params = {name};
		if (serializer.equals(UUIDSerializer.class) || serializer.equals(UUID.class)) {
			//nothing yet
		} else if (serializer.equals(StringSerializer.class) || serializer.equals(String.class)) {
			type = (T) name;
		} else if (serializer.equals(LongSerializer.class) || serializer.equals(Long.class)) {
			type = createObject(Long.class, params);
		} else if (serializer.equals(IntegerSerializer.class) || serializer.equals(Integer.class)) {
			type = createObject(Integer.class, params);
		} else if (serializer.equals(BooleanSerializer.class) || serializer.equals(Boolean.class)) {
			type = createObject(Boolean.class, params);
		} else if (serializer.equals(BytesArraySerializer.class) || serializer.equals(Byte[].class)) {
			type = createObject(Byte[].class, params);
		} else if (serializer.equals(ByteBufferSerializer.class) || serializer.equals(Byte.class)) {
			type = createObject(ByteBuffer.class, params);
		} else if (serializer.equals(DateSerializer.class) || serializer.equals(Date.class)) {
			type = createObject(Date.class, params);
		} else {
			type = createObject(Object.class, params);
		}
		return type;
	}

	/**
	 * Using Java's Reflection mechanism, create an instance of the given class
	 * with the given parameters
	 * @param test The class which contains the tests
	 * @param params The parameter objects to be passed to the constructor
	 * @return T an object of type T determined by objType
	 */
	private <T> T createObject(Class objType, Object[] params) {
		try {
			Class<?> classConstructors = Class.forName(objType.getName());
			Constructor[] allConstructors = classConstructors.getDeclaredConstructors();
			for (Constructor ctor : allConstructors) {
				Class<?>[] pType = ctor.getParameterTypes();
				for (int i = 0; i < pType.length; i++) {
					if (params.length == 0) {
						return (T) ctor.newInstance();
					} else {
						try {
							return (T) ctor.newInstance(params);
						} catch (IllegalArgumentException iae) {
							//occurs if parameters don't match, can ignore for now
						}
					}
				}
			}
		} catch (Exception ex) {
			Logger.getLogger(Column.class.getName()).log(Level.SEVERE, ex.getMessage());
		}
		return null;
	}

	/**
	 * @param name Set the column's name
	 */
	public void setName(String name) {
		this.name = (N) name;
		setChanged(true);
	}

	/**
	 * @param value The column's value
	 */
	public void setValue(String value) {
		this.value = (V) value;
		setChanged(true);
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

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return this.name + ":" + this.value;
	}

	/**
	 * The serializer to be used on this column's name
	 * @param s 
	 */
	public final void setNameSerializer(Serializer s) {
		namese = s;
	}

	/**
	 * The serializer to be used on this column's value
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

	public void setValue(V i) {
		value = i;
	}
}
