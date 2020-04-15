package cn.java666.etlspringboot.controller;

import cn.java666.sztcommon.util.ParseCardNo;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author Geek
 * @date 2020-04-15 13:50:56
 *
 * 卡号明文和密文互转 REST API
 */

@RestController
@RequestMapping("/card")
public class CardController {
	
	@GetMapping("/{no}")
	public String get(@PathVariable String no) {
		return ParseCardNo.parse(no);
	}
}
