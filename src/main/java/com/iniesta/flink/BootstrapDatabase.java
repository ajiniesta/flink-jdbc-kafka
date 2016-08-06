package com.iniesta.flink;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;

/**
 * Loader of a in memory database with random values
 * @author antonio
 *
 */
public class BootstrapDatabase {

	public static final String DB_DRIVER = "org.h2.Driver";
	private static final String DB_FILE = "/tmp/db"+System.currentTimeMillis();
	public static final String DB_CONNECTION = "jdbc:h2:file:"+DB_FILE;//"jdbc:h2:mem:test;DB_CLOSE_DELAY=-1";
	public static final String DB_USER = "sa";
	public static final String DB_PASSWORD = "sa";
	private static final int NUM_ROWS = 1000;

	private static Random random = new Random();
	
	static {
		loadDump();
		File file = new File(DB_FILE);
		file.deleteOnExit();
	}

	private static Connection getConnection() {
		Connection dbConnection = null;
		try {
			Class.forName(DB_DRIVER);
		} catch (ClassNotFoundException e) {
			System.out.println(e.getMessage());
		}
		try {
			dbConnection = DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD);
			return dbConnection;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return dbConnection;
	}

	private static void loadDump() {
		
		String table = "CREATE TABLE RECORDS(ts long, record int)";
		String insert = "INSERT INTO RECORDS(ts, record) values (?,?)";
		
		try(Connection conn = getConnection()){
			createTable(table, conn);
			
			for (int i = 0; i < NUM_ROWS; i++) {
				insertRow(insert, conn);				
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private static void insertRow(String insert, Connection conn) throws SQLException {
		PreparedStatement ps = conn.prepareStatement(insert);
		ps.setLong(1, System.currentTimeMillis()-random.nextInt(100000));
		ps.setInt(2, random.nextInt(1000));
		ps.executeUpdate();
	}

	private static void createTable(String table, Connection conn) throws SQLException {
		PreparedStatement ps = conn.prepareStatement(table);
		ps.executeUpdate();
		ps.close();
	}

	public static int getRowNumber() {
		int rows =  0;
		try(Connection conn = getConnection()){
			String query = "SELECT COUNT(*) FROM RECORDS";
			PreparedStatement ps = conn.prepareStatement(query);
			ResultSet rs = ps.executeQuery();
			if(rs.next()){
				rows = rs.getInt(1);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return rows;
	}

}
