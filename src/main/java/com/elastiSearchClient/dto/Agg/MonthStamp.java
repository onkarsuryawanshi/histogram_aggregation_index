package com.elastiSearchClient.dto.Agg;

import lombok.Data;

import java.time.ZonedDateTime;


@Data
public class MonthStamp {
    private ZonedDateTime fromDate;
    private ZonedDateTime endDate;

    @Override
    public String toString() {
        return "timeStamp{" +
                "fromDate=" + fromDate +
                ", endDate=" + endDate +
                '}';
    }
}
