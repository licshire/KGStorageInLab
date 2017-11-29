package com.cetc28.seu.spark.query.result;

import java.util.ArrayList;
import java.util.List;

import com.cetc28.seu.spark.query.model.coprocessor.SearchRowKeyFinal.Answer;
import com.cetc28.seu.spark.query.model.coprocessor.SearchRowKeyFinal.AnswerInfo;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

public class LocalAnswerSet extends ResultSet{

	private static final long serialVersionUID = 7256652389358789457L;
	private List<ByteString> results;

	public LocalAnswerSet() {
		this.results = new ArrayList<>();
		this.askColumns = new ArrayList<>();
	}

	public List<ByteString> getResults() {
		return results;
	}

	public void setResults(List<ByteString> results) {
		this.results = results;
	}

	@Override
	public void show(int num) {
		for (int i = 0; i < num; i++) {
			if (results.size() == 0) {
				System.out.println("Null");
				break;
			} else if (i < results.size()) {
				Answer res = null;
				try {
					res = Answer.parseFrom(results.get(i));
				} catch (InvalidProtocolBufferException e) {
					e.printStackTrace();
				}
				String row = res.getRow();
				for(AnswerInfo ai : res.getAiList()){
					String cf = ai.getColumnFamily();
					String c = ai.getColumn();
					String value = ai.getValue();
					System.out.println(row+" "+cf+":"+c+" " + value );
				}
				System.out.println();

			}
		}
	}

}
