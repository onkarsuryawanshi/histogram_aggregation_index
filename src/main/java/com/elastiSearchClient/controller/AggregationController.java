package com.elastiSearchClient.controller;


import com.elastiSearchClient.parser.ParseJson;
import com.elastiSearchClient.services.AggregationServices;
import com.elastiSearchClient.services.HistogramServices;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigInteger;
import java.util.Map;
@RestController()
@RequestMapping("/aggregation")
public class AggregationController {
    @Autowired
    private AggregationServices aggregationServices;
    @Autowired
    private HistogramServices histogramServices;
    @Autowired
    private ParseJson parseJson;

    /**
     * return the value of the sum, min, max or average between the 2 entered date
     * according to user enter field
     * **/
    @GetMapping("/aggregationByDateByUserField")
    public ResponseEntity<String> aggregationOnUserField(@RequestParam String startDate, @RequestParam String endDate , @RequestParam String field,@RequestParam String operation){
        Object response = aggregationServices.aggregationByHistogramByUserField(startDate,endDate,field,operation);
        if (response!=null){
            return new ResponseEntity<>(operation +" of " + field +"==>" + response, HttpStatus.OK);
        }
        else{
            return new ResponseEntity<>("Please enter the Correct Operation",HttpStatus.BAD_REQUEST);
        }
    }
    /**histogram operations
     *
     * */

    /** when this end point is hits will get histogram with the fixed interval of the months and user need to specific
    which operation he need to perform on that histogram
    * */
    @GetMapping("/getHistogramByMonth&EnteredOperation")
    public ResponseEntity<String> histogram(@RequestParam String startDate, @RequestParam String endDate , @RequestParam String operation,String field) {
        Object response = histogramServices.createHistogram(startDate, endDate, operation,field);
        if (response != null) {
            return new ResponseEntity<>(operation +"==>"+ response, HttpStatus.OK);
        } else {
            return new ResponseEntity<>("Please enter the Correct Operation", HttpStatus.BAD_REQUEST);
        }
    }

    /**
     * when hits return the date histogram for given operation in range divide into n number of point where n ==> input from user
     * **/

    @GetMapping("/histogramAggregationMultipleField")
    public ResponseEntity<Object> histogramByMultipleField(@RequestParam BigInteger startDate, @RequestParam BigInteger endDate, @RequestParam String operation, @RequestParam Integer points, @RequestBody String jsonString){
        Map<String,String> multipleFields =  parseJson.convertStringToMap(jsonString);
        Object response = histogramServices.createHistogramForMultipleFieldsAndPointsEntered(startDate,endDate,operation,points,multipleFields);
        if (histogramServices !=null) {
            return new ResponseEntity<>(response, HttpStatus.OK);
        }
        else {
            return new ResponseEntity<>("Enter fields properly",HttpStatus.BAD_REQUEST);
        }
    }

}
