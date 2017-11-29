package com.cetc28.seu.spark.query.result;

import java.io.Serializable;
import java.util.List;


public abstract class ResultSet implements Serializable{

	private static final long serialVersionUID = -9154325466263622982L;
	protected List<String> askColumns;
	public abstract void show(int num);
	//public List<Object> visit();
	public List<String> getAskColumns() {
		return askColumns;
	}

	public void setAskColumns(List<String> askColumns) {
		this.askColumns = askColumns;
	}

}
