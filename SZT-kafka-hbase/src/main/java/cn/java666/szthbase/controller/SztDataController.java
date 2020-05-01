package cn.java666.szthbase.controller;

import cn.java666.sztcommon.pojo.SztDataBean;
import cn.java666.szthbase.service.SztDataService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * @author Geek
 * @date 2020-04-30 00:48:34
 * 
 * 控制层，hbase 增删改查
 */
@Slf4j
@RequestMapping("/szt")
@RestController
public class SztDataController {
	
	@Resource
	private SztDataService service;
	
	@PostMapping("/insert")
	public String insert(SztDataBean data) {
		service.insert(data);
		String res = System.currentTimeMillis() + " ------- insert success: " + data;
		log.warn(res);
		return res;
	}
}
