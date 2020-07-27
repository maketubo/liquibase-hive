package liquibase.ext.metastore.utils;

import liquibase.ext.metastore.statement.SetStatement;

public class UserSessionSettings {

    public static SetStatement setStatement(String key, Object value) {
        return new SetStatement(key, value);
    }

}
