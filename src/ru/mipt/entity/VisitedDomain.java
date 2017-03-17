package ru.mipt.entity;

/**
 * Created by dmitry on 18.03.17.
 */
public class VisitedDomain {
    private String domain;
    private int count;

    public VisitedDomain(String domain, int count) {
        this.domain = domain;
        this.count = count;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }
}
