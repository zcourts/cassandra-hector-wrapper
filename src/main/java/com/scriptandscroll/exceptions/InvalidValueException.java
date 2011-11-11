package com.scriptandscroll.exceptions;

/**
 *
 * @author Courtney
 */
public class InvalidValueException extends RuntimeException{
	/**
	 * Useful when invalid values are provided for Cassandra fields, most notably null
	 * @param msg The message to give
	 */
	public InvalidValueException(String msg){
		super(msg);
	}
}
