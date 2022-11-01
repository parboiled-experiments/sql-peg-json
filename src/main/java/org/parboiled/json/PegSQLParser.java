package org.parboiled.json;

import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;

import javax.json.Json;
import javax.json.JsonObject;

import org.parboiled.Action;
import org.parboiled.Rule;
import org.parboiled.annotations.BuildParseTree;
import org.parboiled.annotations.DontLabel;
import org.parboiled.parserunners.ParseRunner;
import org.parboiled.support.ParseTreeUtils;
import org.parboiled.support.ParsingResult;
import org.parboiled.util.ParseUtils;

@BuildParseTree
public class PegSQLParser extends PegParser {

	private static List<String> SUPRESS_LABELS = new LinkedList<>();
	static {
		SUPRESS_LABELS.add("__");
	}

	private static List<String> SUPRESS_SUB_LABELS = new LinkedList<>();
	static {
		SUPRESS_SUB_LABELS.add("_columnNames");
		SUPRESS_SUB_LABELS.add("_columnExpr");
		SUPRESS_SUB_LABELS.add("_tableName");
	}

	private static Rule startRule;

	@Override
	public Rule start() {
		if (startRule == null) {
			InputStream inputStream = getClass().getClassLoader().getResourceAsStream("sql.peg.json");
			JsonObject json = Json.createReader(inputStream).readObject();
			startRule = super.start("sql_stmt", json);
		}
		return startRule;
	}

	@DontLabel
	protected Rule parseRule(JsonObject jsonObj) {

		Rule rule = super.parseRule(jsonObj);

		String type = jsonObj.getString("type");
		if ("rule".equals(type)) {

			String name = jsonObj.getString("name");
			if ("_tableName".equals(name)) {
				rule = Sequence(rule, (Action<?>) context -> !context.getMatch().equalsIgnoreCase("where"));
				RULE_CACHE.put(name, rule);
			}

		} else if ("rule_ref".equals(type)) {

			String name = jsonObj.getString("name");
			if (SUPRESS_LABELS.contains(name)) {
				rule.suppressNode();
			} else if (name.startsWith("KW") || SUPRESS_SUB_LABELS.contains(name)) {
				rule.suppressSubnodes();
			}

		}

		return rule;
	}

	private static void parse(String string) throws Exception {

		ParseRunner<?> runner = ParseUtils.createParseRunner(false, PegSQLParser.class);
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
//		parse(" select * from ( select * from employee ) as worker where rank > 4 ");
//		parse(" select * from ( select * from employee where rank < 4 )");
//		parse(" select * from ( select * from employee where rank < 4 ) toppers");
//		parse(" select * from ( select * from employee where rank < 4 ) as toppers");

	}

}