package cn.java666.etlspringboot.controller;

import cn.java666.sztcommon.util.ParseCardNo;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/card")
@Tag(name = "卡号转换", description = "卡号明文和密文互转 API")
public class CardController {

    @GetMapping("/{no}")
    @Operation(summary = "卡号转换", description = "自动识别卡号明文和密文，一键互转")
    public String get(@PathVariable String no) {
        return ParseCardNo.parse(no);
    }
}
