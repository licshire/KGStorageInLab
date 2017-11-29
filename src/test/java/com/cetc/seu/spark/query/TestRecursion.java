package com.cetc.seu.spark.query;

import java.io.IOException;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.apache.hadoop.hbase.client.HTableInterface;

import com.cetc28.seu.fromAntlr.CustomListener;
import com.cetc28.seu.fromAntlr.ParserException;
import com.cetc28.seu.fromAntlr.SparqlLexer;
import com.cetc28.seu.fromAntlr.SparqlParser;
import com.cetc28.seu.hbase.HbaseConfig;
import com.cetc28.seu.hbase.HbaseTool;
import com.cetc28.seu.spark.query.QueryEngineOnSpark;
import com.cetc28.seu.spark.query.parser.CoprocessorQueryTreeGenerator;

public class TestRecursion {

	public static void main(String[] args) {

		String tableName = HbaseConfig.tableName;
	   	HTableInterface hTable = null;
	   	try {
				 hTable = HbaseTool.getInstance().getTable(tableName);
			} catch (IOException e) {
				e.printStackTrace();
			}
		int count = 0;
	   	while(1 != 0){
	   		String content = "PREFIX vcard: <http://www.w3.org/2001/vcard-rdf/3.0#> " + " PREFIX vname: <http://www.w3.org/2001/vname-rdf/2.0#>"
	   	       		+ "SELECT ?s WHERE { ?s attributes:className zhu0 . ?s attributes:bdnm wayne . }";
	   			      
   	   		ANTLRInputStream input = new ANTLRInputStream(content);
   	   		SparqlLexer lexer = new SparqlLexer(input);
   	           CommonTokenStream tokens = new CommonTokenStream(lexer);
   	           SparqlParser parser = new SparqlParser(tokens);
   	           try{ 
   		            parser.removeErrorListeners();
   		            parser.addErrorListener(new ParserException());
   	           }
   	           catch(Exception e){
   	           	System.err.println("请输入正确的SPARQL格式");
   	           }
   	           ParseTree tree = parser.query(); // begin parsing at query rule

   	           ParseTreeWalker walker = new ParseTreeWalker();
   	           CustomListener sparqlBaseListener = new CustomListener(parser);
   	           walker.walk(sparqlBaseListener, tree);

   	           /**
   	            * coprocessor query
   	            */
   	           CoprocessorQueryTreeGenerator cqGeneration = new CoprocessorQueryTreeGenerator();
   	           sparqlBaseListener.getSelectClause().compile();
   	           cqGeneration.generate(sparqlBaseListener.getSelectClause());
   	           QueryEngineOnSpark qEngine=new QueryEngineOnSpark();
   	           
   	           long start = System.currentTimeMillis();
   	           qEngine.coprocessorRun(cqGeneration.getTree());
   	           long end = System.currentTimeMillis();
   	           System.out.println("last pay out time: " + (end-start) );
	   	       count++;
	   	       if(count >= 5){
	   	    	   break;
	   	       }
	   	}
		

	}

}
