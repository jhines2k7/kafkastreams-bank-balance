package com.hines.james;

import java.sql.Timestamp;
import java.util.Date;

public class DepositEvent {
    private String name;
    private Integer amount;
    private String timestamp;

    public DepositEvent(String name, Integer amount) {
        this.name = name;
        this.amount = amount;

        Date date = new Date();
        Timestamp ts = new Timestamp(date.getTime());

        this.timestamp = ts.toString();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAmount() {
        return amount;
    }

    public void setAmount(Integer amount) {
        this.amount = amount;
    }

    public String getTimestamp() {
        return timestamp;
    }
}
