package com.microsoft.sqlserver.jdbc.variant;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Test;
import org.junit.Assert;

public class SqlVariantTest {
	private Connection getConnection() throws SQLException {
		String url = System.getenv("mssql_jdbc_test_connection_properties");
		if (url == null) {
			Assert.fail("Missing required environment variable: mssql_jdbc_test_connection_properties");
		}
		url += ";sqlVariantAsNull=true";
		return DriverManager.getConnection(url);
	}

	private static interface ResultSetHandler {		
		void process(int n, ResultSet rs) throws SQLException;
	}

	private static final ResultSetHandler FETCH_ALL_COLUMNS = new ResultSetHandler() {
		@Override
		public void process(int n, ResultSet rs) throws SQLException {
			ResultSetMetaData rsmd = rs.getMetaData();
			for(int i=1;i<=rsmd.getColumnCount();i++) {
				rs.getObject(i);
			}
		}
	};

	private void test(String sql, int expectedRowCount, ResultSetHandler handler) throws SQLException {
		try (Connection conn = getConnection()) {
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			ResultSetMetaData rsmd = rs.getMetaData();
			int columnCount = rsmd.getColumnCount();
			int rowCount = 0;
			while (rs.next()) {
				if (handler != null ) {
					handler.process(rowCount, rs);
				}
				rowCount++;
			}
			Assert.assertEquals(expectedRowCount, rowCount);
		}
	}

	@Test
	public void basicSql() throws SQLException {
		String sql = "SELECT 1 AS a, 'abcd' AS b";
		test(sql, 1, FETCH_ALL_COLUMNS);
	}

	@Test
	public void intAsSqlVariant() throws SQLException {
		String sql = "SELECT 1 AS a, CAST(1 AS sql_variant) int_as_variant";
		test(sql, 1, new ResultSetHandler() {
			@Override
			public void process(int n, ResultSet rs) throws SQLException {
				int a = rs.getInt(1);
				Object b = rs.getObject(2);
				Assert.assertEquals(1, a);
				Assert.assertNull(b);
			}
		});
	}

	@Test
	public void textAsSqlVariant() throws SQLException {
		String sql = "SELECT 1 AS a, CAST('test' AS sql_variant) text_as_variant";
		test(sql, 1, new ResultSetHandler() {
			@Override
			public void process(int n, ResultSet rs) throws SQLException {
				int a = rs.getInt(1);
				Object b = rs.getObject(2);
				Assert.assertEquals(1, a);
				Assert.assertNull(b);
			}
		});
	}

	@Test
	public void intAndtextAsSqlVariant() throws SQLException {
		String sql =
		  "SELECT CAST(123 AS sql_variant) AS some_variant" +
		  " UNION ALL " +
		  "SELECT CAST('test' AS sql_variant) AS some_variant";
		test(sql, 1, new ResultSetHandler() {
			@Override
			public void process(int n, ResultSet rs) throws SQLException {
				Object value = rs.getObject(1);
				Assert.assertNull(value);
			}
		});
	}
}
