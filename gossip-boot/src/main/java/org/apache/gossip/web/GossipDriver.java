package org.apache.gossip.web;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.support.SpringBootServletInitializer;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@SpringBootConfiguration
@EnableAutoConfiguration
@SpringBootApplication
public class GossipDriver extends SpringBootServletInitializer {
  
  @RequestMapping("/")
  String home() {
    return "Hello Apache Gossip!";
  }
  
  public static void main(String[] args) throws Exception {
    SpringApplication.run(GossipDriver.class, args);
  }
}