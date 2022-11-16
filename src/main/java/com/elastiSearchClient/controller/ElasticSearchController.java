package com.elastiSearchClient.controller;

import com.elastiSearchClient.services.ElasticSearchServices;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.core.MainResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;


@RestController()
@RequestMapping("/query")
public class ElasticSearchController {

    @Autowired
    private ElasticSearchServices services;

    //    return all documents available on given index
    @GetMapping("/getAllDoc")
    public ResponseEntity<SearchResponse> searchAllDocument() {
        SearchResponse searchResponse = services.getAllDocument();
        return new ResponseEntity<>(searchResponse, HttpStatus.OK);
    }



    @GetMapping("/getDocCount")
    public ResponseEntity<String> countAllDocument() {
        long documentCount = services.getDocumentCount();
        return new ResponseEntity<>("Total Number of Documents are --> " + documentCount, HttpStatus.OK);
    }


    //    returns the all records based on the match field entered
//    field = region , textTobeSearch = Vivo will return all matching records
    @GetMapping("/matchQuery")
    public ResponseEntity<SearchResponse> matchQueryByField(@RequestParam String field, @RequestParam String textTobeSearch) {
        SearchResponse searchResponse = services.getDocumentByMatchQuery(field, textTobeSearch);
        return new ResponseEntity<>(searchResponse, HttpStatus.OK);
    }

    //    returns sum of dataUsed between two Entered dates
    @GetMapping("/sumOfDataUsedBetweenTwoDates")
    public ResponseEntity<String> sumOfDataUsedBetweenTwoDates(@RequestParam String startDate, @RequestParam String endDate) {
        Double searchResponse = services.getSumOfDataUsedBetweenTwoDates(startDate, endDate);
        return new ResponseEntity<>("sum of Dataused Between 2 date ==>" + searchResponse, HttpStatus.OK);
    }


    //    returns avg of dataUsed between two Entered dates
    @GetMapping("/avgOfDataUsedBetweenTwoDates")
    public ResponseEntity<String> avgOfDataUsedBetweenTwoDates(@RequestParam String startDate, @RequestParam String endDate) {
        Double searchResponse = services.getAvgOfDataUsedBetweenTwoDates(startDate, endDate);
        return new ResponseEntity<>("avg data Used between entered date " + searchResponse, HttpStatus.OK);
    }


    //    returns max of dataUsed between two Entered dates
    @GetMapping("/maxOfDataUsedBetweenTwoDates")
    public ResponseEntity<SearchResponse> maxOfDataUsedBetweenTwoDates(@RequestParam String startDate, @RequestParam String endDate) {
        SearchResponse searchResponse = services.getMaxOfDataUsedBetweenTwoDates(startDate, endDate);
        return new ResponseEntity<>(searchResponse, HttpStatus.OK);
    }

    //    returns min of dataUsed between two Entered dates
    @GetMapping("/minOfDataUsedBetweenTwoDates")
    public ResponseEntity<SearchResponse> minOfDataUsedBetweenTwoDates(@RequestParam String startDate, @RequestParam String endDate) {
        SearchResponse searchResponse = services.getMinOfDataUsedBetweenTwoDates(startDate, endDate);
        return new ResponseEntity<>(searchResponse, HttpStatus.OK);
    }





    /*
     *
     * with the field from user
     *
     * */

    @GetMapping("/sumOfFieldsBetweenTwoDates")
    public ResponseEntity<Object> sumOfFields(@RequestParam String startDate, @RequestParam String endDate, @RequestParam String field) {
        SearchResponse searchResponse = services.getSumOfFieldsBetweenTwoDates(startDate, endDate, field);
        return new ResponseEntity<>(searchResponse, HttpStatus.OK);
    }

    @GetMapping("/avgOfFieldsBetweenTwoDates")
    public ResponseEntity<Object> avgOfFields(@RequestParam String startDate, @RequestParam String endDate, @RequestParam String field) {
        Double searchResponse = services.getAvgOfFieldsBetweenTwoDates(startDate, endDate, field);
        return new ResponseEntity<>("Avg of dataUsed ==> " + searchResponse, HttpStatus.OK);
    }

    //    returning the document with the id entered
    @GetMapping("/getById")
    public ResponseEntity<Object> getDocumentById(@RequestParam String doc_Id) {

//        GetResponse response = elasticSearchQuery.getById(doc_Id);
        GetResponse response = services.getById(doc_Id);
        if (response.isExists()) {

            return new ResponseEntity<>(response, HttpStatus.OK);
        } else {
            return new ResponseEntity<>("entered Id " + doc_Id + "doesn't exits...", HttpStatus.NOT_FOUND);
        }
    }

    //    Cluster information can be retrieved using the info() method:
    @GetMapping("/infoLog")
    public MainResponse infoLog() {
        return services.getInfoLog();
    }
    @GetMapping("/radioType")
    public List<String> radioType() {
        return services.getDocumentByRadioType();
    }

    @GetMapping("/termAggByOperator")
    public ResponseEntity operators() {
        Map<String, Long> operatorsAggMap = services.getAggByOperators();
        return new ResponseEntity<>(operatorsAggMap, HttpStatus.OK);
    }

    @GetMapping("/termAggByField")
    public ResponseEntity<Map<String, Long>> termsAggByEnteredField(@RequestParam String field) {
        return new ResponseEntity<>(services.termAggByField(field), HttpStatus.OK);
    }

    //    Histogram Aggregation


    /*
     *
     * write this method to elasticsearchRepositry
     *
     * */
    @GetMapping("/histogramAggregationByMonths")
    public ResponseEntity<Map<Map<String, Double>, Double>> histogramAggregationBYMonth() {
        Map<Map<String, Double>, Double> dateHistogramMap = services.getHistogramAggregationByDate();
        return new ResponseEntity<>(dateHistogramMap, HttpStatus.OK);
    }

    @DeleteMapping("/deleteIndex")
    public ResponseEntity<String> deleteIndex(){
        boolean response = services.deleteIndex();
        if(response){
            return new ResponseEntity<>("Index is deleted Successfully ",HttpStatus.OK);
        }
        else {
            return new ResponseEntity<>("Index Not Found " , HttpStatus.NOT_FOUND);
        }

    }


}
