package com.elastiSearchClient.controller;

import com.elastiSearchClient.parser.ParseJson;
import com.elastiSearchClient.services.HistogramServices;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigInteger;
import java.util.Map;

@RestController()
@RequestMapping("/histogram")
public class HistogramController {
    @Autowired
    private HistogramServices service;
    @Autowired
    private ParseJson parseJson;



    /*hours based fixedInterval(DateHistogramInterval.hours(div))
    * */
    @GetMapping("/getAggregationByEnteredOperationAndPoints")
    public ResponseEntity<String> histogramByPointsAndOperations(@RequestParam String startDate, @RequestParam String endDate ,@RequestParam Integer points, @RequestParam String operation) {
        Map<Map<String, Double>, Double> response = service.createHistogram2(startDate, endDate,points, operation);
        if (response != null) {
            return new ResponseEntity<>(operation + " of dataUsed ==>" + response, HttpStatus.OK);
        } else {
            return new ResponseEntity<>("Please enter the Correct Operation", HttpStatus.BAD_REQUEST);

        }
    }
    /*
    when endpoint got hits will return status based on points

    * */
    @GetMapping("/histogramAggregationByNumberOfPointByStatus")
    public ResponseEntity<Map<String, Map<String, Double>>> histogramAggregation(@RequestParam String startDate, @RequestParam String endDate, @RequestParam Integer points){
        Map<String, Map<String, Double>> dateHistogramMap = service.getHistogramAggregation(startDate,endDate,points);
        return new ResponseEntity<>(dateHistogramMap, HttpStatus.OK);
    }

    /*
    * take multiple field for the input
    * take operation from user
    * data points from user
    *
    *
    *
    * */
    @GetMapping("/histogramAggregationMultipleField")
    public ResponseEntity<Object> histogramByMultipleField(@RequestParam BigInteger startDate, @RequestParam BigInteger endDate,@RequestParam String operation,@RequestParam Integer points, @RequestBody String jsonString){
        Map<String,String> multipleFields =  parseJson.convertStringToMap(jsonString);
       Object response = service.createHistogramForMultipleFieldsAndPointsEntered(startDate,endDate,operation,points,multipleFields);
       if (service!=null) {
           return new ResponseEntity<>(response, HttpStatus.OK);
       }
       else {
           return new ResponseEntity<>("Enter fields properly",HttpStatus.BAD_REQUEST);
       }
    }
}
