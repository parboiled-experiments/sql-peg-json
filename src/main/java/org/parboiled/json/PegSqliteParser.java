package org.parboiled.json;

import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.json.Json;
import javax.json.JsonObject;

import org.parboiled.Action;
import org.parboiled.Rule;
import org.parboiled.annotations.BuildParseTree;
import org.parboiled.annotations.DontLabel;
import org.parboiled.support.ParseTreeUtils;
import org.parboiled.support.ParsingResult;
import org.parboiled.util.ParseUtils;

@BuildParseTree
public class PegSqliteParser extends PegParser {

	protected static Set<String> KEYWORDS = new HashSet<>();
	static {
	}

	private static Map<String, String> VERB_LABELS_MAP = new HashMap<>();
	static {
	}

	private static List<String> OBJECT_LABELS = new LinkedList<>();
	static {
	}

	public static Map<String, String> getVerbLabels() {
		return Collections.unmodifiableMap(VERB_LABELS_MAP);
	}

	public static List<String> getObjectLabels() {
		return Collections.unmodifiableList(OBJECT_LABELS);
	}

	private static List<String> SUPRESS_LABELS = new LinkedList<>();
	static {
	}

	private static List<String> SUPRESS_SUB_LABELS = new LinkedList<>();
	static {
	}

	protected static Rule startRule;

	public Rule start() {
		if (startRule == null) {
			InputStream resourceAsStream = PegSqliteParser.class.getClassLoader()
					.getResourceAsStream("sqlite.peg.json");
			JsonObject json = Json.createReader(resourceAsStream).readObject();
			startRule = super.start("start", json);
		}
		return startRule;
	}

	@DontLabel
	protected Rule parseRule(JsonObject jsonObj) {

		Rule rule = super.parseRule(jsonObj);

		String type = jsonObj.getString("type");
		if ("rule".equals(type)) {

			String name = jsonObj.getString("name");
			if ("ident_name".equals(name) || "column_name".equals(name)) {
				rule = Sequence(rule, (Action<?>) context -> !KEYWORDS.contains(context.getMatch().toUpperCase()));
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
	
	public static ParsingResult<?> parse(String script) throws Exception {
		System.out.println("script : " + script);
		ParsingResult<?> result = ParseUtils.parse(script, PegSqliteParser.class, false);
//		ParsingResult<?> result = ParseUtils.parse(script, PegjsSqliteParser.class, true);
		String parseTreePrintOut = ParseTreeUtils.printNodeTree(result);
		System.out.println("tree : " + parseTreePrintOut);
		return result;
	}

	public static void main(String[] args) throws Exception {

		parse("SELECT * FROM employee");
		
	}

}