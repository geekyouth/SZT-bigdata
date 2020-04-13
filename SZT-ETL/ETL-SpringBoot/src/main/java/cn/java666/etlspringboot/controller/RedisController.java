package cn.java666.etlspringboot.controller;

import cn.java666.etlspringboot.service.RedisService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * @author Geek
 * @date 2020-04-13 21:50:38
 * 
 * redis 查询
 */

@RestController
@RequestMapping("/redis")
public class RedisController {
	
	@Resource
	private RedisService redisService;
	
	@GetMapping("/{key}")
	public String get(@PathVariable String key) {
		return redisService.get(key);
	}
}
