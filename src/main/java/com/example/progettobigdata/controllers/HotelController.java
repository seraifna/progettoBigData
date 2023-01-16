package com.example.progettobigdata.controllers;
import com.example.progettobigdata.dto.Percentuale;
import com.example.progettobigdata.dto.NegativeReviewsPerHotel;

import com.example.progettobigdata.services.HotelService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("hotel")
public class HotelController {

    @Autowired
    HotelService service;


    //per query di prova
    @GetMapping("query1")
    List<NegativeReviewsPerHotel> query1(@RequestParam int offset, @RequestParam int limit) {
        return this.service.queryProva(offset, limit);
    }

    @GetMapping("recensioniNegative")
    List<Percentuale> percentualeNeg(){
        return this.service.Negative();
    }

}
