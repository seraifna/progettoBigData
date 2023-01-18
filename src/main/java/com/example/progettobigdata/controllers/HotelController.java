package com.example.progettobigdata.controllers;
import com.example.progettobigdata.dto.Percentuale;
import com.example.progettobigdata.dto.NegativeReviewsPerHotel;

import com.example.progettobigdata.services.HotelService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;

@RestController
@RequestMapping("hotel")
@CrossOrigin("*")
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

    @GetMapping("getNazionalità")
    HashMap<String, Double> percentualiNazionalità(@RequestParam String hotelName) {
         return this.service.Nazionalità(hotelName);
    }
}
