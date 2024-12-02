package org.example;

import java.sql.*;
import java.util.Arrays;
import java.util.List;

public class Main {
    private static int batchSize = 1_000;
    private static int rowstotal = 15_000;

    public static void main(String[] args) {
        run();
    }

    private static void run() {
        int currentRow = 1;
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3309/sys?zeroDateTimeBehavior=convertToNull&permitMysqlScheme", "root", "admin");
            connection.setAutoCommit(false);
            try {
                createTable(connection);
                PreparedStatement statement = connection.prepareStatement(prepareSqlString());
                while (currentRow < rowstotal) {
                    fillRowData(statement, currentRow);
                    statement.addBatch();
                    if ((currentRow % batchSize)==0) {
                        commit(statement);
                    }
                    currentRow++;
                }
                statement.executeBatch();
                if (!statement.getConnection().getAutoCommit()) {
                    statement.getConnection().commit();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                dropTable(connection);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void commit(PreparedStatement statement) throws SQLException {
        long startTime = System.currentTimeMillis();
        statement.executeBatch();
        statement.getConnection().commit();
        long stopTime = System.currentTimeMillis();
        System.out.println(stopTime - startTime);
    }

    private static void createTable(Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        String tabledef = "CREATE TABLE `test_table` (`column1` TEXT,`column2` TEXT,`column3` TEXT,`column4` TEXT,`column5` TEXT,`column6` TEXT,`column7` TEXT,`column8` TEXT,`column9` TEXT)";
        statement.execute(tabledef);
        statement.close();
        connection.commit();
    }

    private static void dropTable(Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        String dropCommand = "DROP TABLE `test_table`";
        statement.execute(dropCommand);
        statement.close();
        connection.commit();
    }

    private static String prepareSqlString() {
        List<String> names = Arrays.asList("column1", "column2", "column3", "column4", "column5", "column6", "column7", "column8", "column9");

        StringBuilder fill = new StringBuilder();
        StringBuilder columns = new StringBuilder();
        for (String name: names) {
            columns.append(name+",");
            fill.append("?,");
        }


        fill.deleteCharAt(fill.length()-1);
        columns.deleteCharAt(columns.length()-1);
        String sql = "insert into test_table (" + columns + ") values(" + fill + ")";
        return sql;
    }


    private static void fillRowData(PreparedStatement statement, int currentRow) throws SQLException {
        statement.setObject(1, "row " + currentRow, -1);
        statement.setObject(2, "value2", -1);
        statement.setObject(3, "value3", -1);
        statement.setObject(4, "value4", -1);
        statement.setObject(5, "value5", -1);
        statement.setObject(6, "value6", -1);
        statement.setObject(7, "value7", -1);
        statement.setObject(8, "value8", -1);
        statement.setObject(9, "value9", -1);
        currentRow++;
    }
}