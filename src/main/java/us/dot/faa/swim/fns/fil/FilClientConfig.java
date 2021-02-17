package us.dot.faa.swim.fns.fil;

public class FilClientConfig {

    protected String sftpHost;
    protected int sftpPort = 22;
    protected String sftpUsername;
    protected String sftpStrictHostKeyChecking = "no";
    protected String sftpKnownHostsFilePath;
    protected String sftpCertFilePath;
    protected String sftpFilFileDirectoryPath = "";
    
    protected String filDateFileName = "primary_last_date.txt";
    protected String filDataFileTimeFormat = "yyyy-MM-dd HH:mm:ss z";
    protected String filFileName = "initial_load_aixm.xml.gz";
    protected String filFileSavePath = null;

    public String getSftpHost() {
        return this.sftpHost;
    }

    public int getSftpPort() {
        return this.sftpPort;
    }

    public String getSftpUsername() {
        return this.sftpUsername;
    }

    public String getSftpStrictHostKeyChecking() {
        return this.sftpStrictHostKeyChecking;
    }

    public String getSftpKnownHostsFilePath() {
        return this.sftpKnownHostsFilePath;
    }

    public String getSftpCertFilePath() {
        return this.sftpCertFilePath;
    }

    public String getSftpFilFileDirectoryPath() {
        return this.sftpFilFileDirectoryPath;
    }

    public String getFilDateFileName() {
        return this.filDateFileName;
    }
    
    public String getFilDataFileTimeFormat() {
        return this.filDataFileTimeFormat;
    }
    
    public String getFilFileName() {
        return this.filFileName;
    }	
    
    public String getFilFileSavePath() {
        return this.filFileSavePath;
    }

    public FilClientConfig setFilSftpHost(String sftpHost) {
        this.sftpHost = sftpHost;
        return this;
    }

    public FilClientConfig setFilSftpPort(int sftpPort) {
        this.sftpPort = sftpPort;
        return this;
    }

    public FilClientConfig setFilSftpUsername(String sftpUsername) {
        this.sftpUsername = sftpUsername;
        return this;
    }

    public FilClientConfig setFilSftpStrictHostKeyChecking(String sftpStrictHostKeyChecking) {
        this.sftpStrictHostKeyChecking = sftpStrictHostKeyChecking;
        return this;
    }

    public FilClientConfig setFilSftpKnownHostsFilePath(String sftpKnownHostsFilePath) {
        this.sftpKnownHostsFilePath = sftpKnownHostsFilePath;
        return this;
    }

    public FilClientConfig setFilSftpCertFilePath(String sftpCertFilePath) {
        this.sftpCertFilePath = sftpCertFilePath;
        return this;
    }

    public FilClientConfig setFilSftpFilFileDirectoryPath(String sftpFilFileDirectoryPath) {
        this.sftpFilFileDirectoryPath = sftpFilFileDirectoryPath;
        return this;
    }
    
    public FilClientConfig setFilDateFileName(String filDateFileName) {
        this.filDateFileName = filDateFileName;
        return this;
    }
    
    public FilClientConfig setFilDataFileTimeFormat(String filDataFileTimeFormat) {
        this.filDataFileTimeFormat = filDataFileTimeFormat;
        return this;
    }
    
    public FilClientConfig setFilFileName(String filFileName) {
        this.filFileName = filFileName;
        return this;
    }
    
    public FilClientConfig setFilFileSavePath(String filFileSavePath) {
        this.filFileSavePath = filFileSavePath;
        return this;
    }

}
