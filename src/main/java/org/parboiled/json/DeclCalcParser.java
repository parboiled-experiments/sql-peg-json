package org.parboiled.json;

import java.io.StringReader;

import javax.json.Json;
import javax.json.JsonObject;

import org.parboiled.Rule;
import org.parboiled.annotations.BuildParseTree;
import org.parboiled.parserunners.ParseRunner;
import org.parboiled.support.ParseTreeUtils;
import org.parboiled.support.ParsingResult;
import org.parboiled.util.ParseUtils;

@BuildParseTree
public class DeclCalcParser extends DeclParser {

	private static String inputjson = "\n {"//
			+ "\n 	\"expr\" : { \"seq\" : [ \"__\", \"%term%\", \"__\", { \"_0n\" : { \"seq\" : [ { \"AnyOf\" : \"+-\" }, \"__\", \"%term%\", \"__\" ] } } ] }, " //
			+ "\n	\"term\" : { \"seq\" : [ \"%factor%\", \"__\", { \"_0n\" : { \"seq\" : [ { \"AnyOf\" : \"*/\" }, \"__\", \"%factor%\" ] } } ] }, "//
			+ "\n 	\"factor\" : { \"_1of\" : [ \"_d\", { \"seq\":[\"(\",\"%expr%\",\")\" ] } ] } " //
			+ "\n }";

	private static Rule startRule;

	@Override
	public Rule start() {
		if (startRule == null) {
			JsonObject json = Json.createReader(new StringReader(inputjson)).readObject();
			startRule = super.start("expr", json);
		}
		return startRule;
	}

	private static void parse(String string) throws Exception {

		ParseRunner<?> runner = ParseUtils.createParseRunner(false, DeclCalcParser.class);
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