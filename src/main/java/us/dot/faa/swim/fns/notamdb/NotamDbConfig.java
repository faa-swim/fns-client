package us.dot.faa.swim.fns.notamdb;

public class NotamDbConfig {

    protected String driver = "org.h2.Driver";
    protected String connectionUrl = "jdbc:h2:./Notams;mode=MySQL;AUTO_SERVER=TRUE";
    protected String username = "";
    protected String password = "";
    protected String schema = "PUBLIC";
    protected String table = "NOTAMS";    

    public String getDriver() {
        return this.driver;
    }

    public String getConnectionUrl() {
        return this.connectionUrl;
    }

    public String getUsername() {
        return this.username;
    }

    public String getPassword() {
        return this.password;
    }

    public String getSchema() {
        return this.schema;
    }

    public String getTable() {
        return this.table;
    }

    public NotamDbConfig setDriver(String driver) {
        this.driver = driver;
        return this;
    }

    public NotamDbConfig setConnectionUrl(String connectionUrl) {
        this.connectionUrl = connectionUrl;
        return this;
    }

    public NotamDbConfig setUsername(String username) {
        this.username = username;
        return this;
    }

    public NotamDbConfig setPassword(String password) {
        this.password = password;
        return this;
    }

    public NotamDbConfig setSchema(String schema) {
        this.schema = schema;
        return this;
    }

    public NotamDbConfig setTable(String table) {
        this.table = table;
        return this;
    }

}
