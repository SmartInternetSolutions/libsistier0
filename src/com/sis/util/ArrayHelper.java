package com.sis.util;

import java.util.ArrayList;

public class ArrayHelper {
	public static <T> boolean inArrayList(ArrayList<? extends T> array, T other) {
		for (T object : array) {
			if (object.equals(other)) {
				return true;
			}
		}
		
		return false;
	}

	public static <T> boolean inArray(T[] array, T other) {
		for (Object object : array) {
			if (object.equals(other)) {
				return true;
			}
		}
		
		return false;
	}
	
	public static Object[] objectArrayToObjectArray(Object [] objArray) {
		Object [] array = new Object[objArray.length];
		
		int i = 0;
		
		for (Object obj : objArray) {
			array[i++] = obj;
		}
		
		return array;
	}
}
