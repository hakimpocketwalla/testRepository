package org.accenture.password.util;

import java.security.MessageDigest;
import java.security.SecureRandom ;
import java.security.NoSuchAlgorithmException;


/**
 * Used in Authentication
 * 
 */

abstract public class HashUtil {
	
	public static int saltLen = 8;
	static protected String  saltChar = "1234567890abcdefghijklABCDEFGHIJKLMNOPQRSTUVWXYZmnopqrstuvwxyz";
	
	/**
	 * Generates Hash 
	 * @param password
	 * @param salt
	 * @return
	 */
	public static String hashPassword(String password, String salt){	//NOPMD
		MessageDigest md = null;
		if(salt == null){
			salt = getSalt();
		}
		password += salt;
		try {
			md = MessageDigest.getInstance("SHA-256");
		} catch (NoSuchAlgorithmException e) {
			e.getStackTrace();
		}
		StringBuffer hexString = new StringBuffer();
		int count = saltLen*saltLen*saltLen;
		for(int j=0;j<count;j++){
	        md.update(password.getBytes());
	        byte byteData[] = md.digest();
	        hexString = new StringBuffer();
	    	for (int i=0;i<byteData.length;i++) {
	    		String hex=Integer.toHexString(0xff & byteData[i]);
	   	     	if(hex.length()==1) hexString.append('0');
			 	hexString.append(hex);
			}
	    	password = hexString.toString();
		}
    	hexString.append(salt);
		return hexString.toString();
	}
	
	/**
	 * Retrieves Salt
	 * @return
	 */
	public static String getSalt(){
	   StringBuffer randomSalt = new StringBuffer();
	   final SecureRandom rndm = new SecureRandom();
	   while (randomSalt.length() < saltLen) {
		   final int index = (int)(rndm.nextFloat() * saltChar.length());
		   randomSalt.append(saltChar.charAt(index));
	   }
	   return randomSalt.toString();
	}
}
