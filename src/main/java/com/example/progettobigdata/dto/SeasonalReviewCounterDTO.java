package com.example.progettobigdata.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;


@Data
public class SeasonalReviewCounterDTO implements Serializable{

    private int spring = 0, summer = 0,  autumn = 0, winter = 0;

    public void incSeason(String data){
        switch (data) {
            case "Primavera":
                spring++;
                break;
            case "Estate":
                summer++;
                break;
            case "Autunno":
                autumn++;
                break;
            case "Inverno":
                winter++;
                break;
        }//switch
    }


}
