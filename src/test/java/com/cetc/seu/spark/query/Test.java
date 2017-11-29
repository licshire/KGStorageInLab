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

public class Test {

	public static void main(String[] args) {
//      QueryEngineOnSpark qEngine=new QueryEngineOnSpark("HbaseTest40000");
       //外部条件输入
//       Scanner sc = new Scanner(System.in);
//       System.out.println("请输入查询语句");
//       String content = sc.nextLine();
//		try {
//			HbaseTool.getInstance().getTable("testCop_1000_2");
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
		//测试用，添加数据和表
		putData();
		
		String tableName = HbaseConfig.tableName;
   	HTableInterface hTable = null;
   	try {
			 hTable = HbaseTool.getInstance().getTable(tableName);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
//       QueryEngineOnSpark qEngine=new QueryEngineOnSpark("HbaseRegion2Join");
		//两次Join
//		String content = "PREFIX vcard: <http://www.w3.org/2001/vcard-rdf/3.0#> " + " PREFIX vname: <http://www.w3.org/2001/vname-rdf/2.0#>"
//       		+ "SELECT ?s WHERE {?s attributes:className zhu0 . ?s objects:parent ?o .  ?o attributes:className Wayne . ?o objects:parent ?v . ?v attributes:car Lexus}";
		
//		String content = "PREFIX vcard: <http://www.w3.org/2001/vcard-rdf/3.0#> " + " PREFIX vname: <http://www.w3.org/2001/vname-rdf/2.0#>"
//       		+ "SELECT ?s WHERE {  ?o attributes:className Wayne . ?s objects:parent ?o . ?s attributes:className zhu0 .}";
		
//		String content = "PREFIX vcard: <http://www.w3.org/2001/vcard-rdf/3.0#> " + " PREFIX vname: <http://www.w3.org/2001/vname-rdf/2.0#>"
//       		+ "SELECT ?v ?s WHERE { ?s attributes:className zhu0 . ?s objects:parent ?o . ?o attributes:age 24 .}";

//		String content = "PREFIX vcard: <http://www.w3.org/2001/vcard-rdf/3.0#> " + " PREFIX vname: <http://www.w3.org/2001/vname-rdf/2.0#>"
//       		+ "SELECT ?s WHERE { ?s attributes:className zhu0 . }";
   	
//		String content = "PREFIX vcard: <http://www.w3.org/2001/vcard-rdf/3.0#> " + " PREFIX vname: <http://www.w3.org/2001/vname-rdf/2.0#>"
//       		+ "SELECT ?s WHERE { ?s attributes:age 2 . }";
   	
		String content = "PREFIX vcard: <http://www.w3.org/2001/vcard-rdf/3.0#> " + " PREFIX vname: <http://www.w3.org/2001/vname-rdf/2.0#>"
       		+ "SELECT ?s WHERE { ?s attributes:data ?o . }";

//		String content = "PREFIX vcard: <http://www.w3.org/2001/vcard-rdf/3.0#> " + " PREFIX vname: <http://www.w3.org/2001/vname-rdf/2.0#>"
//       		+ "SELECT ?s WHERE {?s attributes:className ?o FILTER (?o = zhu0)}";
   	
//		String content = "PREFIX vcard: <http://www.w3.org/2001/vcard-rdf/3.0#> " + " PREFIX vname: <http://www.w3.org/2001/vname-rdf/2.0#>"
//       		+ "SELECT ?s WHERE {?s attributes:className zhu0. ?s objects:parent ?o. ?o attributes:bdnm Man.}";

//		String content = "PREFIX vcard: <http://www.w3.org/2001/vcard-rdf/3.0#> " + " PREFIX vname: <http://www.w3.org/2001/vname-rdf/2.0#>"
//       		+ "SELECT ?s WHERE {?s attributes:bdnm Man. ?s attributes:className zhu1.}";
		

//		String content = "PREFIX vcard: <http://www.w3.org/2001/vcard-rdf/3.0#> " + " PREFIX vname: <http://www.w3.org/2001/vname-rdf/2.0#>"
//       		+ "SELECT ?p WHERE {?s attributes:bdnm Jack. ?s objects:parent ?o. ?o attributes:bdnm Rose. ?o objects:parent ?p. ?p attributes:bdnm Baby .}";
		
//		String content = "PREFIX vcard: <http://www.w3.org/2001/vcard-rdf/3.0#> " + " PREFIX vname: <http://www.w3.org/2001/vname-rdf/2.0#>"
//       		+ "SELECT ?o WHERE { ?o attributes:bdnm Rose . ?o objects:parent ?p . ?p attributes:bdnm Baby .}";

		
//		String content = "PREFIX vcard: <http://www.w3.org/2001/vcard-rdf/3.0#> " + " PREFIX vname: <http://www.w3.org/2001/vname-rdf/2.0#>"
//       		+ "SELECT ?s WHERE { ?s attributes:className ?o.}";
		
//		String content = "PREFIX vcard: <http://www.w3.org/2001/vcard-rdf/3.0#> " + " PREFIX vname: <http://www.w3.org/2001/vname-rdf/2.0#>"
//		+ "SELECT ?o ?s WHERE { ?s attributes:className zhu0 . ?s objects:parent ?o . ?o attributes:age 1000 .}";

//		String content = "PREFIX vcard: <http://www.w3.org/2001/vcard-rdf/3.0#> " + " PREFIX vname: <http://www.w3.org/2001/vname-rdf/2.0#>"
//		+ "SELECT ?s WHERE { ?s attributes:age ?o FILTER(?o > 50)}";
		
//		String content = "PREFIX vcard: <http://www.w3.org/2001/vcard-rdf/3.0#> " + " PREFIX vname: <http://www.w3.org/2001/vname-rdf/2.0#>"
//       		+ "SELECT ?s WHERE { ?s attributes:className zhu0 . ?s objects:parent ?o . ?o attributes:age 1000 .?s attributes:age ?v .}";

       
          
   		ANTLRInputStream input = new ANTLRInputStream(content);
   		SparqlLexer lexer = new SparqlLexer(input);
           CommonTokenStream tokens = new CommonTokenStream(lexer);
           SparqlParser parser = new SparqlParser(tokens);
           try{ 
	            parser.removeErrorListeners();
	            parser.addErrorListener(new ParserException());
	            //parser.setErrorHandler(new BailErrorStrategy());
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
           qEngine.coprocessorRun(cqGeneration.getTree());

       

       /**
        * Simple query(original)
        */
//       SimpleTreeGenerator stGeneration=new SimpleTreeGenerator();
//       sparqlBaseListener.getSelectClause().compile();
//       stGeneration.generate(sparqlBaseListener.getSelectClause());
////       QueryEngineOnSpark qEngine=new QueryEngineOnSpark("HbaseTest1000000");
//       qEngine.run(stGeneration.getTree());
       
       /**
        * Memory query
        */
//       MemoryQueryTreeGenerator mqGeneration = new MemoryQueryTreeGenerator();
//       sparqlBaseListener.getSelectClause().compile();
//       mqGeneration.generate(sparqlBaseListener.getSelectClause());
//       //QueryEngineOnSpark qEngine=new QueryEngineOnSpark("HbaseRegion3");
//       qEngine.run(mqGeneration.getTree());
       
       /**
        * Index query
        */
//       IndexQueryTreeGenerator itGeneration = new IndexQueryTreeGenerator();
//       sparqlBaseListener.getSelectClause().compile();
//       itGeneration.generate(sparqlBaseListener.getSelectClause());
//       QueryEngineOnSpark qEngine=new QueryEngineOnSpark();
//       qEngine.run(itGeneration.getTree());
       /**
        * hbase filter query
        */
//       FilterQueryTreeGenerator fqGeneration = new FilterQueryTreeGenerator();
//       sparqlBaseListener.getSelectClause().compile();
//       fqGeneration.generate(sparqlBaseListener.getSelectClause());
//       QueryEngineOnSpark qEngine=new QueryEngineOnSpark();
//       qEngine.run(fqGeneration.getTree());

	}

	
	public static void putData() {
		String tableName = HbaseConfig.tableName;
   	try {
			if(!HbaseTool.getAdmin().tableExists(tableName)){
				String[] families = {"objects","attributes","array_objects","index"};
				HbaseTool.createTable(tableName,families);
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
   	
   	HTableInterface hTable = null;
   	try {
			 hTable = HbaseTool.getInstance().getTable(tableName);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
   	
   	//添加小部分数据进行试验
		try {
			HbaseTool.getInstance().putTable();
			//测试一个join
			HbaseTool.getInstance().addOneRecord();
			//测试两个Join
			HbaseTool.getInstance().addTwoJoin();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("start build index");
		HbaseTool.getInstance().buildIndex();
		System.out.println("finished");
	}
}
