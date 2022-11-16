package com.elastiSearchClient.dto;

import lombok.Data;

import java.math.BigInteger;


@Data
public class TimeStamp {
    private BigInteger fromDate;
    private BigInteger toDate;
}
