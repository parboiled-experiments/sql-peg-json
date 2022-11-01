package org.parboiled.json;

import javax.json.Json;
import javax.json.JsonObject;

import org.parboiled.Action;
import org.parboiled.Rule;
import org.parboiled.annotations.BuildParseTree;
import org.parboiled.parserunners.ParseRunner;
import org.parboiled.support.ParseTreeUtils;
import org.parboiled.support.ParsingResult;
import org.parboiled.util.ParseUtils;

@BuildParseTree
public class DeclSQLParser extends DeclParser {

	protected void initLexer() {

		super.initLexer();

		LEXER.put("_tableName", _w);
		LEXER.put("_columnName", _w);
		LEXER.put("_columnValue", _l);
		LEXER.put("_columnNames",
				seq(LEXER.get("_columnName"), __, _0n(seq(__, ',', __, LEXER.get("_columnName")))).suppressSubnodes());
		LEXER.put("_columnExpr", _1of(LEXER.get("_columnName"), LEXER.get("_columnValue")).suppressSubnodes());
		LEXER.put("_op", AnyOf("<>=").label("spec_chars:op"));

	}

	protected void initActions() {
		ACTIONS.put("_nowhere", (Action<?>) context -> !context.getMatch().equalsIgnoreCase("where"));
	}

	private static Rule startRule;

	@Override
	public Rule start() {
		if (startRule == null) {
			JsonObject json = Json.createReader(getClass().getClassLoader().getResourceAsStream("sql.json")).readObject();
			startRule = super.start("sql_stmt", json);
		}
		return startRule;
	}
	
	private static void parse(String string) throws Exception {

		ParseRunner<?> runner = ParseUtils.createParseRunner(false, DeclSQLParser.class);
//		ParseRunner<?> runner = ParseUtils.createParseRunner(true, DeclSQLParser.class);
		ParsingResult<?> result = runner.run(string);

		System.out.println("--------------------------------------------");
		System.out.println(" sql : " + string);
		System.out.println("--------------------------------------------");

		String parseTreePrintOut = ParseTreeUtils.printNodeTree(result);
		System.out.println("tree : " + parseTreePrintOut);

	}

	public static void main(String[] args) throws Exception {

		parse(" select * from employee ");
//
//		parse(" select * from Employee1 ");		
//		parse(" Select * From Employee1 ");
//
//		parse(" Select a From employee ");
//		parse(" Select a , b From employee ");
//		parse(" Select a , b From employee worker ");
//		parse(" Select a , b , c  From employee as worker ");
//		
//		parse(" Select * from employee as toppers where rank > 4");
//		parse(" Select * from employee where rank > 4");
//		
//		parse(" select * from ( select * from employee ) ");
//		parse(" select * from ( select * from employee ) where rank > 4 ");
//		parse(" select * from ( select * from employee ) worker where rank > 4 ");
//		parse(" select * from ( select * from employee where rank < 4 )");
//		parse(" select * from ( select * from employee where rank < 4 ) toppers");
//		parse(" select * from ( select * from employee where rank < 4 ) as toppers");

	}

}