package com.crawler.web;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class DbService {

	private Connection con;

	public DbService() {
		System.out.println("Establishing Connection...");
		try {
			Class.forName("com.mysql.jdbc.Driver");
			this.con = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "root");
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		System.out.println("Connection Established...");
	}

	public ResultSet fetchRecords(String table, Map<String, String> criteria) throws SQLException {
		String sql = "SELECT * FROM " + table + " WHERE ";
		Set<Entry<String, String>> entrySet = criteria.entrySet();
		int count = 0;
		for (Entry<String, String> entry : entrySet) {
			sql += entry.getKey() + "='" + entry.getValue() + "'";
			if (count != entrySet.size() - 1) {
				sql += " AND ";
			} else {
				sql += ";";
			}
			count++;
		}
		Statement sta = con.createStatement();
		return sta.executeQuery(sql);
	}

	public boolean truncateTable(String table) throws SQLException {
		Statement sta = con.createStatement();
		String sql = "TRUNCATE " + table + ";";
		return sta.execute(sql);
	}

	public boolean insertRecord(String table, Map<String, String> criteria) throws SQLException {
		Set<Entry<String, String>> entrySet = criteria.entrySet();
		String keys = "";
		String values = "";
		List<String> valueList = new LinkedList<>();
		int count = 0;
		for (Entry<String, String> entry : entrySet) {
			keys += entry.getKey();
			values += "?";
			valueList.add(entry.getValue());
			if (count != entrySet.size() - 1) {
				keys += ", ";
				values += ", ";
			}
			count++;
		}
		String sql = "INSERT INTO " + table + "(" + keys + ") VALUES " + "(" + values + ");";
		PreparedStatement stmt = con.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
		count = 1;
		for (String value : valueList) {
			stmt.setString(count, value);
			count++;
		}
		return stmt.execute();
	}

	@Override
	protected void finalize() throws Throwable {
		if (con != null || !con.isClosed()) {
			con.close();
		}
	}

}
