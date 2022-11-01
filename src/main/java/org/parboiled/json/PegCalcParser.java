package org.parboiled.json;

import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;

import javax.json.Json;
import javax.json.JsonObject;

import org.parboiled.Rule;
import org.parboiled.annotations.BuildParseTree;
import org.parboiled.annotations.DontLabel;
import org.parboiled.parserunners.ParseRunner;
import org.parboiled.support.ParseTreeUtils;
import org.parboiled.support.ParsingResult;
import org.parboiled.util.ParseUtils;

@BuildParseTree
public class PegCalcParser extends PegParser {

	private static List<String> SUPRESS_LABELS = new LinkedList<>();
	static {
		SUPRESS_LABELS.add("_");
		SUPRESS_LABELS.add("Integer");
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
			InputStream inputStream = getClass().getClassLoader().getResourceAsStream("calc.peg.json");
			JsonObject json = Json.createReader(inputStream).readObject();
			startRule = super.start("Expression", json);
		}
		return startRule;
	}

	@DontLabel
	protected Rule parseRule(JsonObject jsonObj) {

		Rule rule = super.parseRule(jsonObj);

		String type = jsonObj.getString("type");
		if ("rule_ref".equals(type)) {

			String name = jsonObj.getString("name");
			if (SUPRESS_LABELS.contains(name)) {
				rule.suppressNode();
			} else if (SUPRESS_SUB_LABELS.contains(name)) {
				rule.suppressSubnodes();
			}

		}

		return rule;
	}

	private static void parse(String string) throws Exception {

		ParseRunner<?> runner = ParseUtils.createParseRunner(false, PegCalcParser.class);
//		ParseRunner<?> runner = ParseUtils.createParseRunner(true, PegCalcParser.class);

		ParsingResult<?> result = runner.run(string);

		System.out.println("--------------------------------------------");
		System.out.println(" expr : " + string);
		System.out.println("--------------------------------------------");

		String parseTreePrintOut = ParseTreeUtils.printNodeTree(result);
		System.out.println("tree : " + parseTreePrintOut);

	}

	public static void main(String[] args) throws Exception {

//        parse(" 22 + 33 ");
//		parse(" 22 + 33 * 44  ");
//		parse(" 22 + ( 33 * 44 ) ");

		parse(" 22 + ( 33 * 44 ) / 55 ");

	}

}