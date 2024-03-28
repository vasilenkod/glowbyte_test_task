import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import test.task.vasilenko.DataBaseTask;

import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class DataBaseTaskTest {

    private DataBaseTask task;

    @Before
    public void setUp() throws SQLException {
        task = new DataBaseTask();
        task.dropTables();
        task.createTables();
    }

    @Test
    public void bothTablesIsEmpty() throws SQLException {
        String firstTableQuery = ";";
        String secondTableQuery = ";";
        task.insertDataIntoTables(firstTableQuery, secondTableQuery);
        List<String> actual = task.processData();
        List<String> expected = new ArrayList<>();
        Assert.assertEquals(actual, expected);

    }

    @Test
    public void tableNameIsEmpty() throws SQLException {
        String firstTableQuery = ";";
        String secondTableQuery =
                "INSERT INTO TABLE_COLS (TABLE_NAME, COLUMN_NAME, COLUMN_TYPE) " +
                "VALUES " +
                "('users', 'first_name', 'VARCHAR(32)'), " +
                "('users', 'second_name', 'VARCHAR(32)'), " +
                "('users', 'id', 'INT'), " +
                "('users', 'ID', 'VARCHAR(32)'), " +
                "('accounts', 'register_date', 'TIMESTAMP'), " +
                "('accounts', 'CARD_NUMBER', 'INT'), " +
                "('accounts', 'ACCOUNT', 'VARCHAR(32)'), " +
                "('accounts', 'ACCOUNT_ID', 'INT');";
        task.insertDataIntoTables(firstTableQuery, secondTableQuery);
        List<String> actual = task.processData();
        List<String> expected = new ArrayList<>();
        Assert.assertEquals(actual, expected);
    }

    @Test(expected = SQLDataException.class)
    public void tableColsIsEmpty() throws SQLException {
        String firstTableQuery =
                "INSERT INTO TABLE_LIST (TABLE_NAME, PK) " +
                "VALUES " +
                "('users', 'ID'), " +
                "('accounts', 'account, account_id');";
        String secondTableQuery = ";";
        task.insertDataIntoTables(firstTableQuery, secondTableQuery);
        List<String> actual = task.processData();
    }

    @Test
    public void exampleTest() throws SQLException {
        String firstTableQuery =
                "INSERT INTO TABLE_LIST (TABLE_NAME, PK) " +
                "VALUES " +
                "('users', 'ID'), " +
                "('accounts', 'account, account_id');";
        String secondTableQuery =
                "INSERT INTO TABLE_COLS (TABLE_NAME, COLUMN_NAME, COLUMN_TYPE) " +
                        "VALUES " +
                        "('users', 'first_name', 'VARCHAR(32)'), " +
                        "('users', 'second_name', 'VARCHAR(32)'), " +
                        "('users', 'id', 'INT'), " +
                        "('accounts', 'register_date', 'TIMESTAMP'), " +
                        "('accounts', 'CARD_NUMBER', 'INT'), " +
                        "('accounts', 'ACCOUNT', 'VARCHAR(32)'), " +
                        "('accounts', 'ACCOUNT_ID', 'INT');";
        task.insertDataIntoTables(firstTableQuery, secondTableQuery);
        List<String> actual = task.processData();
        List<String> expected = new ArrayList<>();

        expected.add("users, ID, INT");
        expected.add("accounts, account, VARCHAR(32)");
        expected.add("accounts, account_id, INT");

        Assert.assertEquals(actual, expected);
    }

    @Test
    public void generalTest1() throws SQLException {
        String firstTableQuery =
                "INSERT INTO TABLE_LIST (TABLE_NAME, PK) " +
                        "VALUES " +
                        "('users', 'ID'), " +
                        "('accounts', 'account, account_id'), " +
                        "('students', 'first_name, last_name, school_id');";
        String secondTableQuery =
                "INSERT INTO TABLE_COLS (TABLE_NAME, COLUMN_NAME, COLUMN_TYPE) " +
                        "VALUES " +
                        "('users', 'first_name', 'VARCHAR(32)'), " +
                        "('users', 'second_name', 'VARCHAR(32)'), " +
                        "('users', 'id', 'INT'), " +
                        "('accounts', 'register_date', 'TIMESTAMP'), " +
                        "('accounts', 'CARD_NUMBER', 'INT'), " +
                        "('accounts', 'ACCOUNT', 'VARCHAR(32)'), " +
                        "('accounts', 'ACCOUNT_ID', 'INT'), " +
                        "('students', 'FIRST_NAME', 'VARCHAR(32)'), " +
                        "('students', 'last_name', 'VARCHAR(32)'), " +
                        "('students', 'SCHOOL_ID', 'INT');";
        task.insertDataIntoTables(firstTableQuery, secondTableQuery);
        List<String> actual = task.processData();
        List<String> expected = new ArrayList<>();

        expected.add("users, ID, INT");
        expected.add("accounts, account, VARCHAR(32)");
        expected.add("accounts, account_id, INT");
        expected.add("students, first_name, VARCHAR(32)");
        expected.add("students, last_name, VARCHAR(32)");
        expected.add("students, school_id, INT");

        System.out.println(actual);

        Assert.assertEquals(actual, expected);
    }

}
