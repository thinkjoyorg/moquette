package org.eclipse.moquette.spi.impl;

import java.util.Map;

import org.eclipse.moquette.commons.Constants;

/**
 * Utility static methods, like Map get with default value, or elvis operator.
 */
public class Utils {
    public static <T, K> T defaultGet(Map<K, T> map, K key, T defaultValue) {
        T value = map.get(key);
        if (value != null) {
            return value;
        }
        return defaultValue;
    }

	/**
	 * compute backlog of the ringbuffer
	 *
	 * @param p the seq of processed
	 * @param c the seq of current
	 * @return
	 */
	public static long count(long p, long c) {
		long sub = p - c;
		if (sub < 0) {
			sub = Constants.SIZE_RINGBUFFER - (c - p);
		}
		return sub;
	}
}
