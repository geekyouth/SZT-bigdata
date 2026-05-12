package cn.java666.szthbase.controller;

import cn.java666.sztcommon.pojo.SztDataBean;
import cn.java666.szthbase.service.SztDataService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RequestMapping("/szt")
@RestController
@Tag(name = "深圳通数据", description = "HBase 增删改查 API")
public class SztDataController {

    private final SztDataService service;

    public SztDataController(SztDataService service) {
        this.service = service;
    }

    @PostMapping("/insert")
    @Operation(summary = "插入数据", description = "向 HBase 插入深圳通刷卡记录")
    public String insert(SztDataBean data) {
        service.insert(data);
        String res = System.currentTimeMillis() + " ------- insert success: " + data;
        log.warn(res);
        return res;
    }
}
