package com.sunland.schedule.db;

public enum CFHandlerNames {
    DEFAULT("default"), META("meta");

    private String name;

    CFHandlerNames(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
