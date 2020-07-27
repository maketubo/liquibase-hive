package liquibase.ext.metastore.database;

import liquibase.database.Database;
import liquibase.database.jvm.JdbcConnection;
import liquibase.ext.metastore.hive.database.HiveDatabase;
import org.apache.hive.jdbc.HiveConnection;

import java.lang.reflect.Method;

public class HiveDatabaseConnectionWrapper extends JdbcConnection {
    private JdbcConnection conn;

    public HiveDatabaseConnectionWrapper(JdbcConnection conn) {
        super(conn.getUnderlyingConnection());
        this.conn = conn;
    }

    @Override
    public String getURL() {
        return ((HiveConnection) conn.getUnderlyingConnection()).getConnectedUrl();
    }

    @Override
    public String getConnectionUserName() {
        HiveConnection underlyingConnection = (HiveConnection) conn.getUnderlyingConnection();
        try {
            Method getUserName = underlyingConnection.getClass().getMethod("getUserName");
            if(!getUserName.isAccessible()){
                getUserName.setAccessible(true);
            }
            String userName = (String)getUserName.invoke(underlyingConnection);
            return userName;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void attached(Database database) {
        database.addReservedWords(HiveDatabase.RESERVED);
    }
}
