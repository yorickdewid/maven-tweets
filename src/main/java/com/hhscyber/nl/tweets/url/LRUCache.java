/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hhscyber.nl.tweets.url;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Simple LRU Cache using a {@link LinkedHashMap}
 * @author Pedro Oliveira
 *
 * @param <K>
 * @param <V>
 */
public class LRUCache<K, V> extends LinkedHashMap<K, V> {
	private static final long serialVersionUID = 1L;
	private final int defaultSize;

	private LRUCache(final int size) {
		super(size, 0.75f, true);
		this.defaultSize = size;
	}
	
	public static <K, V> LRUCache<K, V> build(int size) {
		return new LRUCache<K, V>(size);
	}

	@Override
	protected boolean removeEldestEntry(Map.Entry<K, V> entry) {
		return size() > defaultSize;
	}
}