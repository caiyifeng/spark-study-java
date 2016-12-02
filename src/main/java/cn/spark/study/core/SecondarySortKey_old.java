package cn.spark.study.core;

import java.io.Serializable;

import scala.math.Ordered;

/*
 * 自定义的二次排序
 * */
public class SecondarySortKey_old implements Ordered<SecondarySortKey_old>,Serializable {


	/**
	 * 
	 */
	private static final long serialVersionUID = -2366006422945129991L;
	
	//首先在自定义的key里面,定义需要进行排序的列
	
	private int first;
	private int second;
	
	
	public SecondarySortKey_old(int first, int second) {
		super();
		this.first = first;
		this.second = second;
	}

	@Override
	public boolean $greater(SecondarySortKey_old other) {
		if (this.first > other.getFirst()) {
			return true;
		} else if (this.first == other.getFirst() &&
				   this.second > other.getSecond()) {
			
			return true;
		}
		return false;
	}

	@Override
	public boolean $greater$eq(SecondarySortKey_old other) {
		// TODO Auto-generated method stub
		if (this.$greater(other)) {
			return true;
		} else if (this.first == other.getFirst() && 
				this.second == other.getSecond()) {
			
			return true;
		}
		return false;
	}

	@Override
	public boolean $less(SecondarySortKey_old other) {
		// TODO Auto-generated method stub
		if (this.first < other.getFirst()) {
			return true;
		} else if (this.first == other.getFirst() && 
				this.second < other.getSecond()) {
			return true;
		}
		return false;
	}

	@Override
	public boolean $less$eq(SecondarySortKey_old other) {
		// TODO Auto-generated method stub
		if (this.$less(other)) {
			return true;
		} else if (this.first == other.getFirst() && 
				   this.second == other.getSecond()) {
			return true;
		}
		return false;
	}

	@Override
	public int compare(SecondarySortKey_old other) {
		if (this.first - other.getFirst() !=0) {
			return this.first - other.getFirst();
		} else {
			return this.second - other.getSecond();
		}
		
	}

	@Override
	public int compareTo(SecondarySortKey_old other) {
		if (this.first - other.getFirst() !=0) {
			return this.first - other.getFirst();
		} else {
			return this.second - other.getSecond();
		}
		
	}

	public int getFirst() {
		return first;
	}

	public void setFirst(int first) {
		this.first = first;
	}

	public int getSecond() {
		return second;
	}

	public void setSecond(int second) {
		this.second = second;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + first;
		result = prime * result + second;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SecondarySortKey_old other = (SecondarySortKey_old) obj;
		if (first != other.first)
			return false;
		if (second != other.second)
			return false;
		return true;
	}
	
	
	
}
