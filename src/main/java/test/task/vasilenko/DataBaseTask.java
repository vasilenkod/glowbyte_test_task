package test.task.vasilenko;

import org.h2.jdbcx.JdbcDataSource;
import java.io.*;
import java.sql.*;
import java.util.*;

public class DataBaseTask {

    public final JdbcDataSource dataSource;

    public DataBaseTask() {
        Map<String, String> db_properties = readProperties();

        String host = db_properties.get("db.host");
        String user = db_properties.get("db.login");
        String password = db_properties.get("db.password");

        this.dataSource = new JdbcDataSource();

        dataSource.setURL(host);
        dataSource.setUser(user);
        dataSource.setPassword(password);
    }


    public void solveTask() throws SQLException {
        //dropTables();
        createTables();

//        String firstTableQuery =
//                "INSERT INTO TABLE_LIST (TABLE_NAME, PK) " +
//                "VALUES " +
//                "('users', 'ID'), " +
//                "('accounts', 'account, account_id');";
//        String secondTableQuery =
//                "INSERT INTO TABLE_COLS (TABLE_NAME, COLUMN_NAME, COLUMN_TYPE) " +
//                "VALUES " +
//                "('users', 'first_name', 'VARCHAR(32)'), " +
//                "('users', 'second_name', 'VARCHAR(32)'), " +
//                "('users', 'id', 'INT'), " +
//                "('accounts', 'register_date', 'TIMESTAMP'), " +
//                "('accounts', 'CARD_NUMBER', 'INT'), " +
//                "('accounts', 'ACCOUNT', 'VARCHAR(32)'), " +
//                "('accounts', 'ACCOUNT_ID', 'INT');";
//
//        insertDataIntoTables(firstTableQuery, secondTableQuery);
        List<String> answer = processData();
        writeIntoFile(answer);
    }


    public void dropTables() throws SQLException {

        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            String dropTableQuery = "DROP TABLE TABLE_LIST;";
            statement.execute(dropTableQuery);

            dropTableQuery = "DROP TABLE TABLE_COLS;";
            statement.execute(dropTableQuery);
        }
    }

    public void createTables() throws SQLException {

        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {

            String createTableQuery =
                    "CREATE TABLE IF NOT EXISTS TABLE_LIST (" +
                        "TABLE_NAME VARCHAR(32), " +
                        "PK VARCHAR(256));";;
            statement.execute(createTableQuery);

            createTableQuery =
                    "CREATE TABLE IF NOT EXISTS TABLE_COLS (" +
                            "TABLE_NAME VARCHAR(32), " +
                            "COLUMN_NAME VARCHAR(32), " +
                            "COLUMN_TYPE VARCHAR(32));";
            statement.execute(createTableQuery);
        }
    }

    public void insertDataIntoTables(String firstTableQuery, String secondTableQuery) throws SQLException {

        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {

            statement.executeUpdate(firstTableQuery);
            statement.executeUpdate(secondTableQuery);
        }
    }

    public List<String> processData() throws SQLException {

        //список для записи ответа
        List<String> data = new ArrayList<>();

        //подготовленный запрос для поиска во второй таблице
        String preparedQuery =
                "SELECT COLUMN_TYPE " +
                "FROM TABLE_COLS " +
                "WHERE TABLE_NAME = ? AND COLUMN_NAME = ? OR COLUMN_NAME = ?;";

        try(Connection connection = dataSource.getConnection();
            Statement statement = connection.createStatement();
            PreparedStatement innerStatement = connection.prepareStatement(preparedQuery)) {

            String selectQueryFromTableList = "SELECT * FROM TABLE_LIST";
            ResultSet tableListData = statement.executeQuery(selectQueryFromTableList);

            //итерируемся по очередному имени базы данных
            while (tableListData.next()) {
                String name = tableListData.getString("TABLE_NAME");
                String pk = tableListData.getString("PK");
                String[] keys = pk.split(", ");

                //проходим по каждому первичному ключу
                for (var key : keys) {

                    //проверяем с учетом регистра
                    String pkLower = key.toLowerCase();
                    String pkUpper = key.toUpperCase();

                    innerStatement.setString(1, name);
                    innerStatement.setString(2, pkLower);
                    innerStatement.setString(3, pkUpper);

                    ResultSet tableColsData = innerStatement.executeQuery();

                    // обрабатываем запрос по второй таблице
                    if (tableColsData.next()) {
                        String columnType = tableColsData.getString("COLUMN_TYPE");
                        String line = name + ", " + key + ", " + columnType;
                        data.add(line);
                    } else {
                        throw new SQLDataException("В TABLE_COLS не нашлось столбца, соответствующего PK в TABLE_NAME");
                    }
                }
            }
        }

        return data;
    }

    public static void writeIntoFile(List<String> lines) {
        try(var fos = new FileWriter("src/main/java/test/task/vasilenko/output.txt")) {
            for (var line : lines) {
                fos.write(line + "\n");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Map<String, String> readProperties() {
        Properties property = new Properties();

        try (var fis = new FileInputStream("src/main/resources/database.properties")) {
            property.load(fis);

            String host = property.getProperty("db.host");
            String login = property.getProperty("db.login");
            String password = property.getProperty("db.password");

            Map<String, String> properties = new HashMap<>();
            properties.put("db.host", host);
            properties.put("db.login", login);
            properties.put("db.password", password);
            return properties;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}