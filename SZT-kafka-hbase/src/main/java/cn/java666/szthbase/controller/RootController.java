package cn.java666.szthbase.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;

@Slf4j
@RestController
@RequestMapping("/")
public class RootController {

    @GetMapping("/")
    public String root() {
        String s = "welcome " + LocalDateTime.now();
        log.warn(s);
        return s;
    }

    @GetMapping("/test")
    public String test() {
        String s = "test " + LocalDateTime.now();
        log.warn(s);
        return s;
    }
}
