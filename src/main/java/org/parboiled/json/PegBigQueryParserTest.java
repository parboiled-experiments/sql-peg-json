package org.parboiled.json;

import java.util.HashMap;
import java.util.HashSet;
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

		//--------------------- SELECT
		
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
			+ "					[('white', '1 year', NULL)]\n"
			+ "WHERE product like '%washer%'\n");
		parseSQL("UPDATE dataset.DetailedInventory\n"
			+ "SET comments = STRUCT<created DATE, comment STRING>('2016-01-01', 'comment1')\n"
			+ "WHERE product like '%washer%'");
		parseSQL("UPDATE dataset.DetailedInventory\n" + "SET specifications = STRUCT<color STRING, warranty STRING, \n"
				+ "					dimensions STRUCT<depth FLOAT64, height FLOAT64, width FLOAT64>>\n"
				+ "					('white', '1 year', NULL)\n" + "WHERE product like '%washer%'");

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

			Map<String, Object> verbObjectMap = prepareVerbObjectMap(result);
			System.out.println("verbObjectMap : " + verbObjectMap);

		} else {
//			System.out.println("error : ");
			throw new RuntimeException("not matched : " + sql);
		}
		System.out.println("--------------------------------------------");

	}

	private static void printTree(ParsingResult<?> result) {

		ParseUtils.visitTree(result.parseTreeRoot, (node, level) -> {
			if (VERB_LABEL_MAPPING.containsKey(node.getLabel()) || OBJECT_LABELS.contains(node.getLabel())) {
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
	private static Map<String, Object> prepareVerbObjectMap(ParsingResult<?> result) {

		Map<String, Object> verbObjectMap = new HashMap<>();

		Stack<Map<String, Object>> verbObjectMapStack = new Stack<>();
		Stack<Integer> verbLevelStack = new Stack<>();

		ParseUtils.visitTree(result.parseTreeRoot, (node, level) -> {

			String label = node.getLabel();
			String verb = VERB_LABEL_MAPPING.get(label);

			if (verb != null) {
				if (verbLevelStack.size() == 0) {
					verbObjectMap.put("verb", verb);
					verbObjectMapStack.push(verbObjectMap);
				} else {
					Map<String, Object> parentVerbObjectMap = verbObjectMapStack.peek();
					List<Map<?, ?>> descMapList = (List<Map<?, ?>>) parentVerbObjectMap.get("descs");
					if (descMapList == null) {
						descMapList = new LinkedList<>();
						verbObjectMap.put("descs", descMapList);
					}
					Map<String, Object> descMap = new HashMap<>();
					descMap.put("verb", verb);
					verbObjectMapStack.push(descMap);
					descMapList.add(descMap);
				}
				verbLevelStack.push(level);

			} else if (OBJECT_LABELS.contains(label)) {
				String object = ParseTreeUtils.getNodeText(node, result.inputBuffer);

				int parentVerbLevel = verbLevelStack.peek();
				Map<String, Object> parentVerbObjectMap = verbObjectMapStack.peek();

				while (level < parentVerbLevel) {
					parentVerbLevel = verbLevelStack.pop();
					parentVerbObjectMap = verbObjectMapStack.pop();
				}

				Set<String> objectList = (Set<String>) parentVerbObjectMap.get("objects");
				if (objectList == null) {
					objectList = new HashSet<>();
					parentVerbObjectMap.put("objects", objectList);
				}
				objectList.add(object);

			}
			return true;
		});
		return verbObjectMap;
	}

}