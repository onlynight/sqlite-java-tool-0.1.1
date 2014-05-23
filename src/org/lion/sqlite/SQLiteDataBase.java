package org.lion.sqlite;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.lion.java.sqlite.hibernate.database.SQLiteDataBaseSession;
import org.lion.java.sqlite.hibernate.table.SQLiteTableHelper;
import org.sqlite.JDBC;

/**
 * the class to connect the database and modify database data.
 * @author onlynight
 *
 */
public class SQLiteDataBase {

	private Connection connection;
	private Statement statement;
	private SQLiteDataBaseSession dataBaseSession;
	
	public SQLiteDataBase( String databaseName , String packateToScan ) {
		this.dataBaseSession = new SQLiteDataBaseSession(databaseName, packateToScan );
		try {
			Class.forName(JDBC.class.getName());
			connection = DriverManager.getConnection(JDBC.PREFIX
					+ dataBaseSession.getDatabaseName());
			statement = connection.createStatement();
			this.createTables();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	public SQLiteDataBase( String databaseName , Class<?>[] models ) {
		this.dataBaseSession = new SQLiteDataBaseSession(databaseName, models);
		try {
			Class.forName(JDBC.class.getName());
			connection = DriverManager.getConnection(JDBC.PREFIX
					+ dataBaseSession.getDatabaseName());
			statement = connection.createStatement();
			this.createTables();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	public Statement getStatement() {
		return statement;
	}

	public boolean isTableExist(String tablename) {
		String sql = dataBaseSession.isTableExist(tablename);
		try {
			System.out.println(sql);
			ResultSet set = statement.executeQuery(sql);
			return set.next();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return false;
	}

	private void createTables() {
		List<String> tables = SQLiteTableHelper.createTables(dataBaseSession
				.getModels().toArray(new Class<?>[] {}));
		for (String sql : tables) {
			boolean mark = this.isTableExist(sql.substring(13,
					sql.indexOf("(") - 1));
			if (mark == false) {
				try {
					System.out.println(sql);
					statement.execute(sql);
				} catch (SQLException e) {
				}
			}
		}
	}

	@SuppressWarnings("unused")
	private void dropTables() {
		List<String> tables = SQLiteTableHelper.dropTables(dataBaseSession
				.getModels().toArray(new Class<?>[] {}));
		for (String table : tables) {
			try {
				System.out.println(table);
				statement.executeUpdate(table);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	public void close() {
		try {
			connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
