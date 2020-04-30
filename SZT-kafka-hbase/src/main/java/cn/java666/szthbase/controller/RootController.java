package cn.java666.szthbase.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;

/**
 * @author Geek
 * @date 2020-04-29 21:11:59
 * 
 * 默认的欢迎页信息，避免 erroe 页面的尴尬...
 */
@Slf4j
@RestController
@RequestMapping("/")
public class RootController {
	
	@GetMapping("/")
	public String root() {
		String s = "welcome " + LocalDateTime.now().toString();
		log.warn(s);
		return s;
	}
	
	@GetMapping("/test")
	public String test() {
		String s = "test " + LocalDateTime.now().toString();
		log.warn(s);
		return s;
	}
}
