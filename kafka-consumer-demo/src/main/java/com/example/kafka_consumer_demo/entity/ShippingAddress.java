package com.example.kafka_consumer_demo.entity;


import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import lombok.Getter;
import lombok.Setter;

@Embeddable
@Getter
@Setter
public class ShippingAddress {

    @Column(name = "shipping_full_name", length = 128)
    private String fullName;

    @Column(name = "shipping_address_line1", length = 256)
    private String addressLine1;

    @Column(name = "shipping_city", length = 100)
    private String city;

    @Column(name = "shipping_postal_code", length = 20)
    private String postalCode;

    @Column(name = "shipping_country", length = 50)
    private String country;
}
