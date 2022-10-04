package tests.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DatabaseProvider {
  public static Connection connection(String dbName) {
    try {
      return DriverManager.getConnection("jdbc:h2:mem:"+dbName+";DB_CLOSE_DELAY=-1", "sa", "");
    } catch (SQLException ex) {
      throw new RuntimeException(ex);
    }
  }
}
