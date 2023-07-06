package org.parboiled.json;

import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;

import javax.json.Json;
import javax.json.JsonObject;

import org.parboiled.Rule;
import org.parboiled.annotations.BuildParseTree;
import org.parboiled.annotations.DontLabel;
import org.parboiled.support.ParseTreeUtils;
import org.parboiled.support.ParsingResult;
import org.parboiled.util.ParseUtils;

@BuildParseTree
public class PegCosmosParser extends PegParser {

	private static List<String> SUPRESS_LABELS = new LinkedList<>();
	static {
	}

	private static List<String> SUPRESS_SUB_LABELS = new LinkedList<>();
	static {
	}

	protected static Rule startRule;

	public Rule start() {
		if (startRule == null) {
			InputStream resourceAsStream = PegCosmosParser.class.getClassLoader()
					.getResourceAsStream("cosmos.peg.json");
			JsonObject json = Json.createReader(resourceAsStream).readObject();
			startRule = super.start("sql", json);
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

	public static ParsingResult<?> parseSQL(String script) throws Exception {
		System.out.println("script : " + script);
		
		ParsingResult<?> result = ParseUtils.parse(script, PegCosmosParser.class, false);
//		ParsingResult<?> result = ParseUtils.parse(script, PegCosmosParser.class, true);
		
//		String parseTreePrintOut = ParseTreeUtils.printNodeTree(result);
//		System.out.println("tree : " + parseTreePrintOut);
		
		return result;
	}

	public static void main(String[] args) throws Exception {

			for(int i=0; i <=10000 ; i++) {

				// -- working
//				parseSQL("SELECT f._rid, a AS orderByItems FROM Families AS f "); // 2-4
//				parseSQL("SELECT f._rid, [a] AS orderByItems FROM Families AS f "); // 5-10
	//	
//				// -- fixed - curly
//				parseSQL("SELECT f._rid, [{\"item\": f.address.city}] AS orderByItems FROM Families AS f "); // 6-10 // 51
//				parseSQL("SELECT f._rid, [{\"item\": f.address.city}] AS orderByItems, {\"givenName\": c.givenName} AS payload \n FROM Families AS f "); // 6-10 // 51
//				parseSQL("SELECT f._rid, [{\"item\": f.address.city}] AS orderByItems, {\"givenName\": c.givenName} AS payload \n FROM Families AS f \n JOIN c IN f.children "); // 7-11 // 51
//				parseSQL("SELECT f._rid, [{\"item\": f.address.city}] AS orderByItems, {\"givenName\": c.givenName} AS payload \n FROM Families AS f \n JOIN c IN f.children \n WHERE f.id = \"WakefieldFamily\""); // 7-13 // 51
//				
//				// - partial working
//				parseSQL("SELECT f._rid, [{\"item\": f.address.city}] AS orderByItems, {\"givenName\": c.givenName} AS payload \n FROM Families AS f \n JOIN c IN f.children \n WHERE (f.id = \"WakefieldFamily\")"); // 14-28
//				parseSQL("SELECT f._rid, [{\"item\": f.address.city}] AS orderByItems, {\"givenName\": c.givenName} AS payload \n FROM Families AS f \n JOIN c IN f.children \n WHERE (f.id = \"WakefieldFamily\") AND true"); // 14-28
//				parseSQL("SELECT f._rid, [{\"item\": f.address.city}] AS orderByItems, {\"givenName\": c.givenName} AS payload \n FROM Families AS f \n JOIN c IN f.children \n WHERE (f.id = \"WakefieldFamily\") AND (true)"); // 17-35
//				
//				// -- fixed - curly + para
//				parseSQL("SELECT f._rid, [{\"item\": f.address.city}] AS orderByItems, {\"givenName\": c.givenName} AS payload \n FROM Families AS f \n JOIN c IN f.children \n WHERE ((f.id = \"WakefieldFamily\"))"); // 14-28 // 70-140
//				parseSQL("SELECT f._rid, [{\"item\": f.address.city}] AS orderByItems, {\"givenName\": c.givenName} AS payload \n FROM Families AS f \n JOIN c IN f.children \n WHERE ((f.id = \"WakefieldFamily\")) AND true"); //14-28 // 70-140
//				parseSQL("SELECT f._rid, [{\"item\": f.address.city}] AS orderByItems, {\"givenName\": c.givenName} AS payload \n FROM Families AS f \n JOIN c IN f.children \n WHERE ((f.id = \"WakefieldFamily\") AND (true))"); // 17-35
//				
//				// -- not working
//				parseSQL("SELECT f._rid, [{\"item\": f.address.city}] AS orderByItems, {\"givenName\": c.givenName} AS payload \n FROM Families AS f \n JOIN c IN f.children \n WHERE ((f.id = \"WakefieldFamily\") AND (true)) \n ORDER BY f.address.city ASC"); // 210

				
//				parseSQL("SELECT {\"Count\": {\"item\": 1 }} AS payload \n FROM c "); // 5
				parseSQL("SELECT [{\"Count\": a }] AS payload \n FROM c "); // 41
//				parseSQL("SELECT {\"Count\": {\"item\": (1) }} AS payload \n FROM c "); // 140 
//				parseSQL("SELECT {\"Count\": {\"item\": Count(1)}} AS payload \n FROM c "); 250
				
				// -- not working
				// parseSQL("SELECT {\"Count\": {\"item\": Count(1)}} AS payload \n FROM c \n JOIN t IN c.tags \n JOIN n IN c.nutrients \n JOIN s IN c.servings \n WHERE (((t.name = \"infant formula\") AND ((n.nutritionValue > 0) AND (n.nutritionValue < 10))) AND (s.amount > 1))");

				
				
//				SELECT TOP 5 (SELECT VALUE Concat('id_', f.id)) AS id FROM food f

			}

		}
}