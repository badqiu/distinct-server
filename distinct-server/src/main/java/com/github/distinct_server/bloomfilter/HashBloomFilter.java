package com.github.distinct_server.bloomfilter;

import java.util.Arrays;
import java.util.Collection;

public class HashBloomFilter {

	private int hashNum;
	private BloomFilter[] bf;
	
	public void clear() {
		for(BloomFilter item : bf) {
			item.clear();
		}
	}

	public void add(Object element) {
		int index = indexByHash(element.hashCode());
		bf[index].add(element);
	}

	private int indexByHash(int hashCode) {
		return Math.abs(hashCode) % bf.length;
	}

	public void add(byte[] bytes) {
		int index = indexByHash(Arrays.hashCode(bytes));
		bf[index].add(bytes);
	}

	public void addAll(Collection c) {
		for(Object item : c) {
			add(item);
		}
	}

	public boolean contains(Object element) {
		int index = indexByHash(element.hashCode());
		return bf[index].contains(element);
	}

	public boolean contains(byte[] bytes) {
		int index = indexByHash(Arrays.hashCode(bytes));
		return bf[index].contains(bytes);
	}

	public <E> boolean containsAll(Collection<E> c) {
		 for (E element : c) {
			 if (!contains(element))
	        	return false;
		 }
		 return true;
	}

	public long count() {
		long sum = 0;
		for(BloomFilter item : bf) {
			sum += item.count();
		}
		return sum;
	}

	public void cleanChange() {
		for(BloomFilter item : bf) {
			item.cleanChange();
		}
	}

//	public int notContainsCountAndAdd(Collection keys) {
//		return bf[0].notContainsCountAndAdd(prefix, keys);
//	}
	
}
