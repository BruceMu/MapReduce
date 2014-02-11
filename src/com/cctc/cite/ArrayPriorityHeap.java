package com.cctc.cite;

import java.lang.reflect.Array;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;


public class ArrayPriorityHeap <KeyType extends Comparable,ValueType> implements Iterable<Map.Entry<KeyType, ValueType>>{
	private int size;
	private Class<KeyType> typek;
	private Class<ValueType> typev;
	private PairEntry<KeyType,ValueType>[] arrayheap;
	private int point_n;
	
	public static class PairEntry <KeyType,ValueType> implements Map.Entry<KeyType, ValueType>{
		private KeyType key;
		private ValueType value;
		public PairEntry(KeyType k,ValueType v){
			this.key = k;
			this.value =  v;
		}

		@Override
		public KeyType getKey() {
			// TODO Auto-generated method stub
			return this.key;
		}

		@Override
		public ValueType getValue() {
			// TODO Auto-generated method stub
			return this.value;
		}

		@Override
		public ValueType setValue(ValueType value) {
			// TODO Auto-generated method stub
			this.value = value;
			return value;
		}
		
	}
	
	@SuppressWarnings("unchecked")
	public ArrayPriorityHeap(int n,Class<KeyType> type1,Class<ValueType> type2){
		this.size = n;
		try {
			Class type = new PairEntry<KeyType,ValueType>(type1.newInstance(),type2.newInstance()).getClass();
			arrayheap = (PairEntry<KeyType,ValueType>[])Array.newInstance(type, size+2);
			point_n = 1;
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public boolean insertHeap(KeyType kelem,ValueType velem){
		if(point_n > size) return false;
		PairEntry<KeyType, ValueType> entry = new PairEntry<KeyType,ValueType>(kelem,velem);
		arrayheap[point_n] = entry;
		swimSort(point_n);
		point_n ++;
		return true;
	}
	
	public PairEntry<KeyType,ValueType> deHeap(){
		PairEntry<KeyType,ValueType> tmp = arrayheap[1];
		arrayheap[1] = arrayheap[point_n-1];
		arrayheap[point_n--] = null;
		sinkSort(1);
		return tmp;
	}
	
	
	private void swimSort(int index){
		if(index == 1) return ;
		int p_index = index/2;
		//子节点比父节点小
		if(arrayheap[index].getKey().compareTo(arrayheap[p_index].getKey())<0){
			exech(index,p_index);
			swimSort(p_index);
		}
	}
	
	private void sinkSort(int index){
		int s_index = index *2;
		int tem_index = s_index;
		if(s_index >= point_n) return;
		int s2_index = s_index +1;
		if(s2_index < point_n){
			if(arrayheap[s2_index].getKey().compareTo(arrayheap[s_index].getKey())<0)
				tem_index = s2_index;
		}
		//父节点比子节点大
		if(arrayheap[index].getKey().compareTo(arrayheap[tem_index].getKey())>0){
			exech(index,tem_index);
			sinkSort(tem_index);
		}
	}
	
	private void exech(int index1,int index2){
		PairEntry<KeyType,ValueType> exetmp = arrayheap[index1];
		arrayheap[index1] = arrayheap[index2];
		arrayheap[index2] = exetmp;
	}

	@Override
	public Iterator<Entry<KeyType, ValueType>> iterator() {
		// TODO Auto-generated method stub
		return new HeapEntryIterator();
	}
	
	private class HeapEntryIterator implements Iterator<Map.Entry<KeyType, ValueType>>{
		private int i = point_n-1;
		@Override
		public boolean hasNext() {
			// TODO Auto-generated method stub
			return i>=1;
		}

		@Override
		public Entry<KeyType, ValueType> next() {
			// TODO Auto-generated method stub
			return arrayheap[i--];
		}

		@Override
		public void remove() {
			// TODO Auto-generated method stub
		}
	}
}
