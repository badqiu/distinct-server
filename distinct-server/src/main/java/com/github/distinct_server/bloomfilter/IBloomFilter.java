package com.github.distinct_server.bloomfilter;

import java.util.Collection;

/**
 * BloomFilter接口
 * @author badqiu
 *
 */
public interface IBloomFilter {
	
	public void clear();

	public void add(Object element);

	public void add(byte[] bytes);

	public void addAll(Collection c);

	public boolean contains(Object element) ;

	public boolean contains(byte[] bytes) ;

	public <E> boolean containsAll(Collection<E> c);

	public long count();

	public void cleanChange();
	
}
