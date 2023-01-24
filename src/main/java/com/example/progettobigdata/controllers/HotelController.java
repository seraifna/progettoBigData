package com.example.progettobigdata.controllers;
import com.example.progettobigdata.dto.AverageWordCounter;
import com.example.progettobigdata.dto.Percentuale;
import com.example.progettobigdata.dto.NegativeReviewsPerHotel;

import com.example.progettobigdata.dto.SeasonalReviewCounterDTO;
import com.example.progettobigdata.services.HotelService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

@RestController
@RequestMapping("hotel")
@CrossOrigin("*")
public class HotelController {

    @Autowired
    HotelService service;


    //per query di prova
//    @GetMapping("query1")
//    List<NegativeReviewsPerHotel> query1(@RequestParam int offset, @RequestParam int limit) {
//        return this.service.queryProva(offset, limit);
//    }

    //per un hotel passato come parametro, la percentuale di recensioni pos, neg, neutre
    @GetMapping("percentualiRecensioni")
    List<Percentuale> percentualeReviews(String hotelName){
        return this.service.Percentuals(hotelName);
    }


    //Dato il nome di un hotel, mi restituisce la percentuale di recensori inglesi, francesi, arabi ecc.
    @GetMapping("getNazionalità")
    HashMap<String, Double> percentualiNazionalità(@RequestParam String hotelName) {
         return this.service.Nazionalità(hotelName);
    }

    //restituisce il numero medio di parole che tendono a lasciare recensori di ogni nazionalità
    @GetMapping("mostAccurateReviews")
    List<AverageWordCounter> getAccurateReviews() {
        return this.service.Accurate();
    }

    //la uso per prendermi tutti i tag che compaiono nelle recensioni
    @GetMapping("getTags")
    HashSet<String> getTag(){
        return this.service.Tags();
    }


    //restituisce, data una lista di tag, la lista degli hotel in cui compaiono recensioni con quei tag
    @PostMapping("getHotelByTags")
    HashSet<String> getHotelByTags(@RequestBody List<String> tags){
        return this.service.getByTags(tags);
    }

    //Restituisce, dato un hotel, il numero di recensioni per ogni stagione
    @GetMapping("getSeason")
    SeasonalReviewCounterDTO getSeasonReview(String hotelName) {
        return this.service.getSeason(hotelName);
    }

}
