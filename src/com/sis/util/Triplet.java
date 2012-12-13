package com.sis.util;

public class Triplet<A, B, C> {
	private final A a;
	private final B b;
	private final C c;
	
	public Triplet(A a, B b, C c) {
		this.a = a;
		this.b = b;
		this.c = c;
	}
	
	public A getAlpha() {
		return a;
	}
	
	public B getBeta() {
		return b;
	}
	
	public C getCharlie() {
		return c;
	}

	public int hashCode() {
		return a.hashCode() ^ b.hashCode() ^ c.hashCode();
	}
	
	public boolean equals(Object o) {
		if (o == null || !(o instanceof Triplet)) {
			return false;
		}
		
		@SuppressWarnings("unchecked")
		Triplet<A, B, C> pair = (Triplet<A, B, C>) o;
		
		return pair.a.equals(a) && pair.b.equals(b) && pair.c.equals(c);
	}
}
