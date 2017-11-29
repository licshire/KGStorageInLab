package com.hbase.test;

public class TestFunc {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//System.out.println(judgeAllNumber("12A"));
		String input = "2010-11-13 11:27:00";
		String s = "^-?\\d+$";
		String d = "(\\d{1,4}[-|\\/|年|\\.]\\d{1,2}[-|\\/|月|\\.]\\d{1,2}([日|号])?(\\s)*(\\d{1,2}([点|时])?((:)?\\d{1,2}(分)?((:)?\\d{1,2}(秒)?)?)?)?(\\s)*(PM|AM)?)";
		
		System.out.println(input.matches(d));
		
	}
	public static boolean judgeAllNumber(String str){
		boolean judge = true;
		for(int i = 0; i < str.length(); i++){
			char c = str.charAt(i);
			if(!Character.isDigit(c)){
				judge = false;
				break;
			}
		}
		return judge;
	}
}
