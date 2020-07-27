package liquibase.ext.metastore.configuration;

import liquibase.configuration.AbstractConfigurationContainer;

/*
 *  config container properties for global
 *  for example: u can provide a driver and use in HiveDatabase
 *
 */
public class HiveMetastoreConfiguration extends AbstractConfigurationContainer {
    private static final String LOCK = "lock";

    public HiveMetastoreConfiguration() {
        super("liquibase");
        getContainer().addProperty(LOCK, Boolean.class)
                .setDescription("Should Liquibase lock database while executing")
                .setDefaultValue(true)
                .addAlias("lock");
    }

    public boolean getLock() {
        return getContainer().getValue(LOCK, Boolean.class);
    }

    public HiveMetastoreConfiguration setLock(boolean noLock) {
        getContainer().setValue(LOCK, noLock);
        return this;
    }
}
