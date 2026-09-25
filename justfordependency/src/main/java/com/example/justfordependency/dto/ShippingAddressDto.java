package com.example.justfordependency.dto;

import lombok.Data;

@Data
public class ShippingAddressDto {

    private String fullName;


    private String addressLine1;


    private String city;


    private String postalCode;


    private String country;
}
