package com.et.monotone;

public interface IdGenerator {

	/**
	 * @return A guaranteed unique 64bit integer that is monotonically increasing in value.
	 */
	long nextId();

}