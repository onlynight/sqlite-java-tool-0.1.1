package org.lion.sqlite;

import java.lang.reflect.ParameterizedType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.lion.java.sqlite.hibernate.table.SQLiteTableDAO;
import org.lion.java.sqlite.hibernate.table.SQLiteTableSession;

/**
 * implements some basic database operation.
 * 
 * @author onlynight
 * 
 * @param <T>
 */
public class BasicDAO<T> extends SQLiteTableDAO<T> {

	private SQLiteTableSession tableSession;
	private SQLiteDataBase database;

	@SuppressWarnings("unchecked")
	public BasicDAO(SQLiteDataBase database, String tablename) {
		super();
		this.database = database;
		this.tableSession = new SQLiteTableSession(
				((Class<T>) ((ParameterizedType) super.getClass()
						.getGenericSuperclass()).getActualTypeArguments()[0]),
				tablename);
	}

	@Override
	public boolean isTableExist() {
		String sql = tableSession.isTableExist();
		try {
			System.out.println(sql);
			ResultSet set = database.getStatement().executeQuery(sql);
			return set.next();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public void insert(T entity) {
		String sql = tableSession.insert(entity);
		try {
			System.out.println(sql);
			database.getStatement().executeUpdate(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void insert(List<T> entities) {
		String sql = tableSession.insert(entities);
		try {
			System.out.println(sql);
			database.getStatement().executeUpdate(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void delete(T entity) {
		String sql = tableSession.delete(entity);
		try {
			System.out.println(sql);
			database.getStatement().executeUpdate(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void delete(List<T> entities) {
		String sql = tableSession.delete(entities);
		try {
			System.out.println(sql);
			database.getStatement().executeUpdate(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void update(T entity) {
		String sql = tableSession.update(entity);
		try {
			System.out.println(sql);
			database.getStatement().executeUpdate(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void update(List<T> entities) {
		String sql = tableSession.update(entities);
		try {
			System.out.println(sql);
			database.getStatement().executeUpdate(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public List<T> select(String column, String value) {
		String sql = tableSession.select(column, value);
		try {
			System.out.println(sql);
			ResultSet set = database.getStatement().executeQuery(sql);
			return getObjectList(set);
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public List<T> select(String[] columns, String[] values) {
		String sql = tableSession.select(columns, values);
		try {
			System.out.println(sql);
			ResultSet set = database.getStatement().executeQuery(sql);
			return getObjectList(set);
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public List<T> selectAll() {
		String sql = tableSession.selectAll();
		try {
			System.out.println(sql);
			ResultSet set = database.getStatement().executeQuery(sql);
			return getObjectList(set);
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void execute(String sql) {
		String temp = tableSession.execute(sql);
		try {
			System.out.println(temp);
			database.getStatement().execute(temp);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public List<T> executeQurey(String sql) {
		String temp = tableSession.selectAll();
		try {
			System.out.println(temp);
			ResultSet set = database.getStatement().executeQuery(temp);
			return getObjectList(set);
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public List<T> select(T entity) {
		String sql = tableSession.select(entity);
		try {
			System.out.println(sql);
			ResultSet set = database.getStatement().executeQuery(sql);
			return getObjectList(set);
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public long count() {
		String sql = tableSession.count();
		try {
			System.out.println(sql);
			ResultSet set = database.getStatement().executeQuery(sql);
			set.next();
			return set.getLong( 1 );
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return 0;
	}
}
