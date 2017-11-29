package com.cetc28.seu.rdf;

import java.io.Serializable;

public class StringLiteral implements Term,Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String value;
	
	public StringLiteral(String value) {
		this.value=value;
	}

	@Override
	public String getValue() {
		return value;
	}

	@Override
	public boolean isVariable() {
		if(value.startsWith("?")){
			return true;
		}
		return false;
	}
	
}
