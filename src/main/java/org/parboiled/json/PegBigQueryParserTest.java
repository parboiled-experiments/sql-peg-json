package org.parboiled.json;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.parboiled.parserunners.ParseRunner;
import org.parboiled.support.ParseTreeUtils;
import org.parboiled.support.ParsingResult;
import org.parboiled.util.ParseUtils;

public class PegBigQueryParserTest {

	public static void main(String[] args) throws Exception {

		// --------------------- WITH

		parseSQL("WITH locations AS "
				+ "(SELECT ARRAY<STRUCT<city STRING, state STRING>>[(\"Seattle\",\"Washington\"), "
				+ "(\"Phoenix\", \"Arizona\")] AS location) " + "SELECT l.LOCATION[offset(0)].* "
				+ "FROM locations l");
		parseSQL("WITH Roster AS " + "(SELECT 'Adams' as LastName, 50 as SchoolID ) " + "SELECT * FROM Roster");
		parseSQL("WITH Roster AS "
				+ "(SELECT 'Adams' as LastName, 50 as SchoolID UNION ALL SELECT 'Buchanan', 52 UNION ALL SELECT 'Davis', 51 ) "
				+ "SELECT * FROM Roster");
		parseSQL("WITH Roster AS " + "(SELECT 'Adams' as LastName, 50 as SchoolID UNION ALL SELECT 'Buchanan', 52) "
				+ "SELECT * FROM Roster");

		parseSQL("WITH groceries AS " + "(SELECT \"milk\" AS dairy, " + "\"eggs\" AS protein, "
				+ "\"bread\" AS grain) " + "SELECT gg.* " + "FROM groceries AS gg");
		parseSQL("WITH locations AS " + "(SELECT STRUCT(\"Seattle\" AS city, \"Washington\" AS state) AS location "
				+ "UNION ALL " + "SELECT STRUCT(\"Phoenix\" AS city, \"Arizona\" AS state) AS location) "
				+ "SELECT l.location.* " + "FROM locations l");
		parseSQL("WITH orders AS " + "(SELECT 5 as order_id, " + "\"sprocket\" as item_name, " + "200 as quantity) "
				+ "SELECT * EXCEPT (order_id) " + "FROM orders");
		parseSQL("WITH orders AS " + "(SELECT 5 as order_id, " + "\"sprocket\" as item_name, " + "200 as quantity) "
				+ "SELECT * REPLACE (\"widget\" AS item_name) " + "FROM orders");

		parseSQL("WITH\n" + "  subQ11 AS (SELECT * FROM Roster WHERE SchoolID = 52)\n"
				+ ", subQ22 AS (SELECT SchoolID FROM subQ1)\n" + "SELECT DISTINCT * FROM subQ2;");

		parseSQL("WITH " + " subQ1 AS (SELECT SchoolID FROM Roster), "
				+ " subQ2 AS (SELECT OpponentID FROM PlayerStats) "
				+ " SELECT * FROM subQ1 UNION ALL SELECT * FROM subQ2\n");

		parseSQL("WITH TeamMascot AS\n" + " (SELECT 50 as SchoolID, 'Jaguars' as Mascot UNION ALL\n"
				+ "  SELECT 51, 'Knights' UNION ALL\n" + "  SELECT 52, 'Lakers' UNION ALL\n"
				+ "  SELECT 53, 'Mustangs')\n" + "SELECT * FROM TeamMascot");

		parseSQL("WITH " 
				+ " vals AS ( SELECT 1 x, 'a' y " 
				+ "			UNION ALL SELECT 1 x, 'b' y "
				+ "			UNION ALL SELECT 2 x, 'a' y " + "			UNION ALL SELECT 2 x, 'c' y ) "
				+ " SELECT x, ARRAY_AGG(y) as array_agg FROM vals GROUP BY x");

		parseSQL("WITH Items AS (SELECT [\"coffee\", \"tea\", \"milk\"] AS item_array)\n"
				+ "SELECT item_array,  item_array[OFFSET(1)] AS item_offset,  item_array[ORDINAL(1)] AS item_ordinal,  item_array[SAFE_OFFSET(6)] AS item_safe_offset FROM Items");

		parseSQL("WITH Words AS (\n" + "  SELECT 'Intend' as value, 'east' as direction UNION ALL\n"
				+ "  SELECT 'Secure', 'north' UNION ALL\n" + "  SELECT 'Clarity', 'west'\n" + " )\n"
				+ "SELECT EXISTS ( SELECT value FROM Words WHERE direction = 'south' ) as result;");

		parseSQL("WITH t AS (SELECT 'column value' AS `current_timestamp`)\n"
				+ "SELECT current_timestamp() AS now, t.current_timestamp FROM t;");

		parseSQL("WITH\n" + "  Precipitation AS (\n" 
				+ "    SELECT 2001 AS year, 'spring' AS season, 9 AS inches\n"
				+ "    UNION ALL    SELECT 2001, 'winter', 1  \n" 
				+ "    UNION ALL    SELECT 2000, 'fall', 3    \n"
				+ "    UNION ALL    SELECT 2000, 'summer', 5  \n" 
				+ "    UNION ALL    SELECT 2000, 'spring', 7  \n"
				+ "    UNION ALL    SELECT 2000, 'winter', 2 )\n"
				+ "SELECT AVG(inches HAVING MAX year) AS average FROM Precipitation;");

		// --------------------- SELECT
		
		parseSQL("SELECT LastName " + "FROM Roster \n" + "UNION ALL\n" + "SELECT LastName " + "FROM PlayerStats");
		parseSQL("SELECT LastName " + "FROM Roster \n" + "UNION DISTINCT\n" + "SELECT LastName " + "FROM PlayerStats");
		
		parseSQL("SELECT * " + "FROM " + "Roster " + "JOIN " + "UNNEST(" + "ARRAY(" + "SELECT AS STRUCT * "
				+ "FROM PlayerStats " + "WHERE PlayerStats.OpponentID = Roster.SchoolID" + ")) AS PlayerMatches "
				+ "ON PlayerMatches.LastName = 'Buchanan'");
		parseSQL("SELECT Roster.LastName, TeamMascot.Mascot\n"
				+ "FROM Roster JOIN TeamMascot ON Roster.SchoolID = TeamMascot.SchoolID");
		parseSQL("SELECT Roster.LastName, TeamMascot.Mascot\n" + "FROM Roster CROSS JOIN TeamMascot");
		parseSQL("SELECT Roster.LastName, TeamMascot.Mascot\n" + "FROM Roster, TeamMascot");

		parseSQL("SELECT STRUCT(1, 2) FROM Users");
		parseSQL("SELECT ARRAY(SELECT STRUCT(1 AS A, 2 AS B)) FROM Users");
		parseSQL("SELECT IF(STARTS_WITH(Users.username, \"a\")," + "NULL, STRUCT(1, 2)) FROM Users");

		parseSQL("SELECT * FROM (SELECT \"apple\" AS fruit, \"carrot\" AS vegetable)");
		parseSQL("SELECT SELECT IF(STARTS_WITH(Users.username, \"a\")," + "NULL, STRUCT(1, 2)) FROM Users1 FROM Users2");
		parseSQL("SELECT SELECT IF(STARTS_WITH(Users.username, \"a\")," + "NULL, STRUCT(1, 2)) FROM Users1");
		parseSQL("SELECT ARRAY(SELECT IF(STARTS_WITH(Users.username, \"a\")," + "NULL, STRUCT(1, 2)) FROM Users1) FROM Users2");
		parseSQL("SELECT ARRAY(SELECT IF(STARTS_WITH(Users.username, \"a\")," + "NULL, STRUCT(1, 2))) FROM Users2");

		parseSQL("SELECT ARRAY_CONCAT_AGG(x) AS array_concat_agg FROM (\n" + "		  SELECT [NULL, 1, 2, 3, 4] AS x\n"
				+ "		  UNION ALL SELECT NULL\n" + "		  UNION ALL SELECT [5, 6]\n"
				+ "		  UNION ALL SELECT [7, 8, 9]\n" + "		);");

		parseSQL(
				"SELECT  COUNT(*) AS total_count,  COUNT(fruit) AS non_null_count,  MIN(fruit) AS min,MAX(fruit) AS max\n"
						+ "FROM\n" + "  (\n" + "    SELECT NULL AS fruit\n" + "    UNION ALL\n"
						+ "    SELECT 'apple' AS fruit\n" + "    UNION ALL\n" + "    SELECT 'pear' AS fruit\n"
						+ "    UNION ALL\n" + "    SELECT 'orange' AS fruit\n" + "  )");

		parseSQL("SELECT * FROM (SELECT -1 AS x) WHERE x > 0 AND ERROR('Example error');");

		// ----------------- INSERT

		parseSQL("INSERT INTO Singers (SingerId, FirstName, LastName) "
				+ "SELECT SingerId, FirstName, LastName FROM AckworthSingers");
		parseSQL("INSERT INTO Singers (SingerId, FirstName, LastName) "
				+ "SELECT * FROM UNNEST ([(4, 'Lea', 'Martin'),(6, 'Elena', 'Campbell')])");

		parseSQL("INSERT INTO Singers (SingerId, FirstName) "
				+ " VALUES (4, (SELECT FirstName FROM AckworthSingers WHERE SingerId = 4))");
		parseSQL("INSERT INTO Singers (SingerId, FirstName, LastName) " + "VALUES (4, "
				+ "(SELECT FirstName FROM AckworthSingers1 WHERE SingerId = 4), "
				+ "(SELECT LastName FROM AckworthSingers2 WHERE SingerId = 4))");

		// ----------------- DELETE

		parseSQL("DELETE FROM target_name WHERE true");
		parseSQL("DELETE FROM Singers WHERE FirstName = 'Alice'");
		parseSQL("DELETE FROM Singers\n" + "WHERE\n FirstName NOT IN (SELECT FirstName from AckworthSingers)");

		// ----------------- UPDATE

		parseSQL("UPDATE Singers\n" + "SET BirthDate = '1990-10-10'\n"
				+ "WHERE FirstName = 'Marc' AND LastName = 'Richards'");
		parseSQL("UPDATE Concerts SET TicketPrices = [25, 50, 100]\n" + " WHERE VenueId = 1");

		// ----------------- DDL

		parseSQL("CREATE DATABASE first-db");
		parseSQL("create SCHEMA testInfo");

		parseSQL("CREATE TABLE Singers (\n" + "SingerId INT64 NOT NULL,\n" + "FirstName STRING(1024),\n"
				+ "LastName STRING(1024),\n" + "SingerInfo BYTES(MAX),\n" + "BirthDate DATE\n"
				+ ") PRIMARY KEY(SingerId)");
		parseSQL("CREATE TABLE Singers (\n" + "SingerId INT64 NOT NULL,\n" + "FirstName STRING(1024),\n"
				+ "LastName STRING(1024),\n" + "SingerInfo BYTES(MAX),\n" + "BirthDate DATE\n"
				+ ")");

		// ----------------- Enhancement

		parseSQL("SELECT s.SingerId, s.FirstName, s.LastName, s.SingerInfo, c.ConcertDate\n"
				+ "FROM Singers@{FORCE_INDEX=SingersByFirstLastName} AS s JOIN\n"
				+ "Concerts@{FORCE_INDEX=ConcertsBySingerId} AS c ON s.SingerId = c.SingerId\n"
				+ "WHERE s.FirstName = \"Catalina\" AND s.LastName > \"M\"");

		parseSQL("SELECT MessageId FROM Messages TABLESAMPLE BERNOULLI (0.1 PERCENT)");
		parseSQL("SELECT Subject FROM\n"
				+ "(SELECT MessageId, Subject FROM Messages WHERE ServerId=\"test\")\n"
				+ "TABLESAMPLE BERNOULLI(50 PERCENT)\n"
				+ "WHERE MessageId > 3");

		parseSQL("SELECT A.name, item, ARRAY_LENGTH(A.items) item_count_for_name\n"
				+ "FROM\n"
				+ "UNNEST(\n"
				+ "[STRUCT('first' AS name, [1, 2, 3, 4] AS items),\n"
				+ "STRUCT('second' AS name, [] AS items)]) AS A\n"
				+ "LEFT JOIN A.items AS item");

		parseSQL("SELECT FirstName FROM Singers ORDER BY FirstName COLLATE \"en_CA\"");
		parseSQL("SELECT FirstName, LastName FROM Singers ORDER BY FirstName COLLATE \"en_US\" ASC,LastName COLLATE \"ar_EG\" DESC");

		parseSQL("SELECT * from 'BigQuery.order' limit 111");

		parseSQL("SELECT first_name, last_name FROM Stu1; " + //
				"SELECT first_name, last_name FROM Stu2; " + //
				"SELECT first_name, last_name FROM Stu3 " + //
				"SELECT first_name, last_name FROM Stu4");

		parseSQL("SELECT first_name, last_name FROM Stu;" + //
				"Delete From Course where id = 3;" + //
				"SELECT * FROM Course;" + //
				"Update Stu Set first_name = 'adi' where id = 1;" + //
				"Select student_id From Stu_Course;");

		parseSQL("SELECT table_name, ddl FROM `bigquery-public-data`.census_bureau_usa.INFORMATION_SCHEMA.TABLES");
		parseSQL("SELECT table_name, ddl FROM `bigquery-public-data`.census_bureau_usa.INFORMATION_SCHEMA.TABLES WHERE table_name = 'population_by_zip_2010'");
		parseSQL("SELECT * FROM charged-mind-281913.SYdataset.books");
		parseSQL("SELECT * FROM `charged-mind-281913.SYdataset.books`");
		parseSQL("SELECT * FROM `charged-mind-281913.SYdataset.books` as test FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)");

		// ------------------ BUG FIXES

		parseSQL("SELECT LastName " + "FROM Roster " + "EXCEPT \n" + "SELECT LastName " + "FROM PlayerStats");
		parseSQL("SELECT LastName " + "FROM Roster " + "EXCEPT ALL\n" + "SELECT LastName " + "FROM PlayerStats");
		parseSQL("SELECT LastName " + "FROM Roster " + "EXCEPT DISTINCT\n" + "SELECT LastName " + "FROM PlayerStats");

		parseSQL("SELECT LastName " + "FROM Roster " + "INTERSECT \n" + "SELECT LastName " + "FROM PlayerStats");
		parseSQL("SELECT LastName " + "FROM Roster " + "INTERSECT ALL\n" + "SELECT LastName " + "FROM PlayerStats");
		parseSQL("SELECT LastName " + "FROM Roster " + "INTERSECT DISTINCT " + "SELECT LastName " + "FROM PlayerStats");

		parseSQL("create table `BigQueryE2E.Order`(id int64,name string)");

		parseSQL("insert into BigQueryE2E.Ordered (id,name) values (101,\"sandeep\")");
		parseSQL("INSERT Singers " + "SELECT SingerId, FirstName, LastName FROM AckworthSingers");

		parseSQL("UPDATE Singers\n" + "SET BirthDate = '1990-10-10' "
				+ "From Singers "
				+ "WHERE FirstName = 'Marc' AND LastName = 'Richards'");

		parseSQL("UPDATE dataset.DetailedInventory\n"
				+ "SET comments = ARRAY<STRUCT<created DATE, comment STRING>>[(CAST('2016-01-01' AS DATE), 'comment1')]\n"
				+ "WHERE product like '%washer%'");
		parseSQL("UPDATE dataset.DetailedInventory\n"
				+ "SET specifications = ARRAY<STRUCT<color STRING, warranty STRING, \n"
				+ "					dimensions STRUCT<depth FLOAT64, height FLOAT64, width FLOAT64>>>\n"
				+ "					[('white', '1 year', NULL)]\n" + "WHERE product like '%washer%'\n");
		parseSQL("UPDATE dataset.DetailedInventory\n"
				+ "SET comments = STRUCT<created DATE, comment STRING>('2016-01-01', 'comment1')\n"
				+ "WHERE product like '%washer%'");
		parseSQL("UPDATE dataset.DetailedInventory\n" + "SET specifications = STRUCT<color STRING, warranty STRING, \n"
				+ "					dimensions STRUCT<depth FLOAT64, height FLOAT64, width FLOAT64>>\n"
				+ "					('white', '1 year', NULL)\n" + "WHERE product like '%washer%'");

		parseSQL("SELECT  " //
				+ " TIMESTAMP_TRUNC(\"2015-06-15 00:00:00+00\", ISOYEAR) AS isoyear_boundary, " //
				+ " EXTRACT(ISOYEAR FROM TIMESTAMP \"2015-06-15 00:00:00+00\") AS isoyear_number " //
				+ " From test");

		parseSQL("SELECT TIMESTAMP(\"2008-12-25 15:30:00+00\") AS timestamp_str from test;");
	}

	private static Map<String, String> VERB_LABEL_MAPPING = PegBigQueryParser.getVerbLabels();
	private static List<String> OBJECT_LABELS = PegBigQueryParser.getObjectLabels();

	public static void parseSQL(String sql) throws Exception {

		System.out.println("--------------------------------------------");
		System.out.println("sql : " + sql);
		System.out.println("--------------------------------------------");

		ParseRunner<?> runner = ParseUtils.createParseRunner(false, PegBigQueryParser.class);
//		ParseRunner<?> runner = ParseUtils.createParseRunner(true, PegBigQueryParser.class);

		ParsingResult<?> result = runner.run(sql);

		if (result.parseErrors.size() == 0) {

			System.out.println("tree : ");
			printTree(result);

			List<Map<String, Object>> verbObjectMapList = prepareVerbObjectMap(result);
			System.out.println("verbObjectMapList : " + verbObjectMapList);

			verbObjectMapList.forEach(PegBigQueryParserTest::cleanVerbs);
			System.out.println("verbObjectMapList-cleaned : " + verbObjectMapList);

		} else {
			throw new RuntimeException("not matched : " + sql);
		}
		System.out.println("--------------------------------------------");

	}

	@SuppressWarnings("unchecked")
	private static Map<?, ?> cleanVerbs(Map<?, ?> verbObjectMap) {

		List<Map<?, ?>> descMapList = (List<Map<?, ?>>) verbObjectMap.get("descs");
		if (descMapList != null) {
			for (Iterator<?> iterator = descMapList.iterator(); iterator.hasNext();) {
				Map<?, ?> descMap = (Map<?, ?>) iterator.next();

				descMap = cleanVerbs(descMap);
				if (descMap == null) {
					iterator.remove();
				}

			}
		}
		if (descMapList == null || descMapList.isEmpty()) {
			verbObjectMap.remove("descs");
			Set<String> objectList = (Set<String>) verbObjectMap.get("objects");
			if (objectList == null) {
				return null;
			}
		}

		return verbObjectMap;

	}

	private static void printTree(ParsingResult<?> result) {

		ParseUtils.visitTree(result.parseTreeRoot, (node, level) -> {
			if (VERB_LABEL_MAPPING.containsKey(node.getLabel()) //
					|| OBJECT_LABELS.contains(node.getLabel()) //
					|| "SEMICOLON".equals(node.getLabel())) {
				System.out.print(level + " : ");
				for (int i = 0; i < level; i++) {
					System.out.print(" ");
				}
				String value = ParseTreeUtils.getNodeText(node, result.inputBuffer).trim();
				System.out.println(node.getLabel() + " : " + value);
			}
			return true;
		});
	}

	@SuppressWarnings("unchecked")
	private static List<Map<String, Object>> prepareVerbObjectMap(ParsingResult<?> result) {

		List<Map<String, Object>> verbObjectMapList = new ArrayList<>();
		Stack<Integer> verbLevelStack = new Stack<>();
		Stack<Map<String, Object>> verbObjectMapStack = new Stack<>();

		ParseUtils.visitTree(result.parseTreeRoot, (node, level) -> {

			String label = node.getLabel();
			String verb = VERB_LABEL_MAPPING.get(label);

			if (verb != null) {
				if (verbLevelStack.size() == 0) {
					Map<String, Object> verbObjectMap = new HashMap<>();
					verbObjectMap.put("verb", verb);
					verbObjectMapStack.push(verbObjectMap);
					verbObjectMapList.add(verbObjectMap);
				} else {
					Map<String, Object> parentVerbObjectMap = getParentMap(level, verbLevelStack, verbObjectMapStack);
					List<Map<?, ?>> descMapList = (List<Map<?, ?>>) parentVerbObjectMap.get("descs");
					if (descMapList == null) {
						descMapList = new LinkedList<>();
						parentVerbObjectMap.put("descs", descMapList);
					}
					Map<String, Object> descMap = new HashMap<>();
					descMap.put("verb", verb);
					verbObjectMapStack.push(descMap);
					descMapList.add(descMap);
				}
				verbLevelStack.push(level);

			} else if (OBJECT_LABELS.contains(label)) {
				String object = ParseTreeUtils.getNodeText(node, result.inputBuffer);

				Map<String, Object> parentVerbObjectMap = getParentMap(level, verbLevelStack, verbObjectMapStack);
				Set<String> objectList = (Set<String>) parentVerbObjectMap.get("objects");
				if (objectList == null) {
					objectList = new HashSet<>();
					parentVerbObjectMap.put("objects", objectList);
				}
				objectList.add(object);

			} else if ("SEMICOLON".equals(label)) {
				verbLevelStack.clear();
				verbObjectMapStack.clear();
			}
			return true;
		});
		return verbObjectMapList;
	}

	public static Map<String, Object> getParentMap(Integer level, Stack<Integer> verbLevelStack,
			Stack<Map<String, Object>> verbObjectMapStack) {
		int parentVerbLevel = verbLevelStack.peek();
		Map<String, Object> parentVerbObjectMap = verbObjectMapStack.peek();

		if (level < parentVerbLevel) {
			while (level < parentVerbLevel) {
				parentVerbLevel = verbLevelStack.pop();
				parentVerbObjectMap = verbObjectMapStack.pop();
			}
			verbLevelStack.push(parentVerbLevel);
			verbObjectMapStack.push(parentVerbObjectMap);
		}
		return parentVerbObjectMap;
	}

}