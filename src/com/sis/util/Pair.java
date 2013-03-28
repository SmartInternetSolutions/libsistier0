package com.sis.util;

public class Pair<L, R> {
	private final L l;
	private final R r;
	
	public Pair(L l, R r) {
		this.l = l;
		this.r = r;
	}
	
	public L getLeft() {
		return l;
	}
	
	public R getRight() {
		return r;
	}

	public int hashCode() {
		return l.hashCode() ^ r.hashCode();
	}
	
	public boolean equals(Object o) {
		if (o == null || !(o instanceof Pair)) {
			return false;
		}
		
		@SuppressWarnings("unchecked")
		Pair<L, R> pair = (Pair<L, R>) o;
		
		return pair.l.equals(l) && pair.r.equals(r);
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		
		if (l == null) {
			sb.append("(null)");
		} else {
			sb.append(l.toString());
		}
		
		if (r == null) {
			sb.append(", (null)");
		} else {
			sb.append(", ");
			sb.append(r.toString());
		}
		
		return sb.toString();
	}
}
