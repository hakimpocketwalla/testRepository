package org.accenture.password.util;


public class PasswordGenerator {

	public static void main(String[] args) {
		
		// provide user details
		String userName = "atlanta.user";
		String fullName = "atlanta user";
		String inputPassword = "password";
		String email = "";
		
		
		String storedPassword = "b15e7cf9321019eef746072acb2a5d30138b31a2050be41106fe1d5bba6a1abfYFby1MWz"; 
		String storedSalt = storedPassword.substring(storedPassword.length()- HashUtil.saltLen);
		String newPassword = HashUtil.hashPassword(inputPassword, storedSalt);
		
		System.out.println(inputPassword);
		System.out.println("INSERT INTO cec_authentication VALUES");
		System.out.println("("+8+",'" + userName + "','" + newPassword + "','" + email + "','" + fullName + "',"+true+")");
		
		
	}

}
