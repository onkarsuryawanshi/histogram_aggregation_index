package com.elastiSearchClient.exception;

import org.elasticsearch.ElasticsearchException;



//class should extends to proper exception like elasticsearch exception in this case
//super class will not work like Exception
public class InvalidInputException extends ElasticsearchException {
    public InvalidInputException(String message) {
        super(message);
    }
}
