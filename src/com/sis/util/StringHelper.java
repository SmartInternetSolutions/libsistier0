package com.sis.util;

import java.io.StringWriter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;

import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.lang.StringEscapeUtils;
import org.w3c.dom.Node;

/**
 * miscellaneous helper for String related tasks
 * 
 * @author CR
 *
 */
public class StringHelper {
    private static final char DEFAULT_TRIM_WHITESPACE = ' ';
    
	private static final DateFormat databaseDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	public static String databaseDate() {
		return databaseDate(new Date());
	}
	
	public static String databaseDate(Date date) {
		return databaseDateFormat.format(date);
	}
	
	public static Date createDateByDatabaseDate(String databaseDate) {
		return createDateByDatabaseDate(databaseDate, new Date());
	}
	
	public static Date createDateByDatabaseDate(String databaseDate, Date defaultValue) {
		try {
			return databaseDateFormat.parse(databaseDate);
		} catch (ParseException e) {
			return defaultValue;
		}
	}
	
	public static String calculateMD5(java.lang.String input) {
		MessageDigest md5;
		try {
			md5 = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			return null;
		}
		md5.reset();
		md5.update(input.getBytes());
		byte[] result = md5.digest();
		
		StringBuffer hexString = new StringBuffer();
        for (int i=0; i<result.length; i++) {
            hexString.append(String.format("%02X", 0xFF & result[i]));
        }
        
        return hexString.toString();
	}
	
	public static String[] objectArrayToStringArray(Object [] objArray) {
		String [] array = new String[objArray.length];
		
		int i = 0;
		
		for (Object obj : objArray) {
			array[i++] = obj.toString();
		}
		
		return array;
	}
	
	/**
	 * @deprecated use {@link ArrayHelper#objectArrayToObjectArray(Object[])} instead
	 * @param objArray
	 * @return
	 */
	public static Object[] objectArrayToObjectArray(Object [] objArray) {
		return ArrayHelper.objectArrayToObjectArray(objArray);
	}
	
	public static String coalesce(Object[] coll, Character seperator) {
		StringBuilder sb = new StringBuilder();
		
		for (Object object : coll) {
			if (object == null) {
				continue;
			}
			
			if (sb.length() > 0) {
				sb.append(seperator);
			}
			
			sb.append(object.toString());
		}
		
		return sb.toString();
	}
	public static String coalesce(Collection<?> coll, Character seperator) {
		StringBuilder sb = new StringBuilder();
		
		for (Object object : coll) {
			if (object == null) {
				continue;
			}
			
			if (sb.length() > 0) {
				sb.append(seperator);
			}
			
			sb.append(object.toString());
		}
		
		return sb.toString();
	}

	/**
	 * @deprecated use {@link ArrayHelper#inArrayList(ArrayList, Object)} instead
	 */
	public static boolean inArrayList(ArrayList<?> strings, String string) {
		for (Object object : strings) {
			if (object.toString().equals(string)) {
				return true;
			}
		}
		
		return false;
	}

	/**
	 * @deprecated use {@link ArrayHelper#inArray(Object[], Object)} instead
	 */
	public static boolean inArray(String[] strings, String string) {
		for (Object object : strings) {
			if (object.toString().equals(string)) {
				return true;
			}
		}
		
		return false;
	}
	
	public static String escapeHtml(String input) {
		return StringEscapeUtils.escapeHtml(input);
	}
	
	/**
	 * Taken from http://www.coderanch.com/how-to/java/DocumentToString
	 * @param node
	 * @return
	 */
    public static String xmlToString(Node node) {
        try {
            Source source = new DOMSource(node);
            StringWriter stringWriter = new StringWriter();
            Result result = new StreamResult(stringWriter);
            
            TransformerFactory factory = TransformerFactory.newInstance();
            Transformer transformer = factory.newTransformer();
            transformer.transform(source, result);

            return stringWriter.getBuffer().toString();
        } catch (TransformerConfigurationException e) {
        } catch (TransformerException e) {
        }
        return null;
    }
    
    // following six methods are taken from http://www.java2s.com/Code/Java/Data-Type/Trimstringfromleftorright.htm
    
    /**
     * Concatonates all the strings given together into one long string.
     * @param strings An array of strings, cannot be null and cannot contain null.
     * @return A string made by concatonating all the elements of strings together.
     */
    public static String stringConcat(String ... strings)
    {
        /* Add up the total length of the strings, this is a small optimization
         * for when were working with lots of long strings. */
        int length = 0;
        for (int i = 0; i < strings.length; i++) {
            length += strings[i].length();
        }

        // append all strings together
        final StringBuilder concatString = new StringBuilder(length);
        for (int i = 0; i < strings.length; i++) {
            concatString.append(strings[i]);
        }
        
        return concatString.toString();
    }

    /**
     * Trims spaces from the left side of the string and returns the result.
     * @param string The string to trim.
     * @return A string with all spaces removed from the left side.
     */
    public static String trimLeft(String string)
    {
        return trimLeft( string, DEFAULT_TRIM_WHITESPACE);
    }

    /**
     * Trims the character given from the given string and returns the result.
     * @param string The string to trim, cannot be null.
     * @param trimChar The character to trim from the left of the given string.
     * @return A string with the given character trimmed from the string given.
     */
    public static String trimLeft(final String string, final char trimChar)
    {
        final int stringLength = string.length();
        int i;
        
        for (i = 0; i < stringLength && string.charAt(i) == trimChar; i++) {
            /* increment i until it is at the location of the first char that
             * does not match the trimChar given. */
        }

        if (i == 0) {
            return string;
        } else {
            return string.substring(i);
        }
    }

    /**
     * Trims spaces from the right side of the string given and returns the
     * result.
     * @param string The string to trim, cannot be null.
     * @return A string with all whitespace trimmed from the right side of it.
     */
    public static String trimRight(final String string)
    {
        return trimRight(string, DEFAULT_TRIM_WHITESPACE);
    }

    /**
     * Trims the character given from the right side of the string given. The
     * result of this trimming is then returned.
     * @param string The string to trim, cannot be null.
     * @param trimChar The character to trim from the right side of the given string.
     * @return The result of trimming the character given from the right side of the given string.
     */
    public static String trimRight(final String string, final char trimChar)
    {
        final int lastChar = string.length() - 1;
        int i;

        for (i = lastChar; i >= 0 && string.charAt(i) == trimChar; i--) {
            /* Decrement i until it is equal to the first char that does not
             * match the trimChar given. */
        }
        
        if (i < lastChar) {
            // the +1 is so we include the char at i
            return string.substring(0, i+1);
        } else {
            return string;
        }
    }

    /**
     * Trims the character given from both left and right of the string given.
     * For trimming whitespace you can simply use the String classes trim method.
     * @param string The string to trim characters from, cannot be null.
     * @param trimChar The character to trim from either side of the given string.
     * @return A string with the given characters trimmed from either side.
     */
    public static String trim(final String string, final char trimChar)
    {
        return trimLeft(trimRight(string, trimChar), trimChar);
    }
}
