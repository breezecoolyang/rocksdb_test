package com.sunland.schedule.config;

public class ChronosConfig implements ConfigurationValidator {
    private String clusterName;
    private String groupName;
    private boolean pullOn;
    private boolean pushOn;
    private boolean deleteOn;
    private boolean standAlone;
    private boolean fakeSend;

    private DbConfig dbConfig;

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public boolean isPullOn() {
        return pullOn;
    }

    public void setPullOn(boolean pullOn) {
        this.pullOn = pullOn;
    }

    public boolean isPushOn() {
        return pushOn;
    }

    public void setPushOn(boolean pushOn) {
        this.pushOn = pushOn;
    }

    public boolean isDeleteOn() {
        return deleteOn;
    }

    public void setDeleteOn(boolean deleteOn) {
        this.deleteOn = deleteOn;
    }

    public boolean isStandAlone() {
        return standAlone;
    }

    public void setStandAlone(boolean standAlone) {
        this.standAlone = standAlone;
    }

    public DbConfig getDbConfig() {
        return dbConfig;
    }

    public void setDbConfig(DbConfig dbConfig) {
        this.dbConfig = dbConfig;
    }


    public boolean isFakeSend() {
        return fakeSend;
    }

    public void setFakeSend(boolean fakeSend) {
        this.fakeSend = fakeSend;
    }

    @Override
    public boolean validate() {
        return true;
    }

    @Override
    public String toString() {
        return "ChronosConfig{" +
                "clusterName='" + clusterName + '\'' +
                ", groupName='" + groupName + '\'' +
                ", pullOn=" + pullOn +
                ", pushOn=" + pushOn +
                ", deleteOn=" + deleteOn +
                ", standAlone=" + standAlone +
                ", fakeSend=" + fakeSend +
                ", dbConfig=" + dbConfig +
                '}';
    }
}
