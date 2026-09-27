package com.example.justfordependency.dto;

import lombok.Data;

@Data
public class ShippingAddressRequest {


    private String street;
    private String city;
    private String postalCode;
    private String country;

}
