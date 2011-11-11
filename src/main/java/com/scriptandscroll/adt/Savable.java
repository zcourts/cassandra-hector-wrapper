package com.scriptandscroll.adt;

/**
 *A savable object is simply one that can have changes applied to it that can 
 * then be written back to Cassandra.
 * It's main purpose is to provide common ground for things such as column and super columns
 * that are so similar but still vastly different both in implementation and concept (to some extent)
 * @author Courtney
 */
public interface Savable {
	public boolean isChanged();
}
