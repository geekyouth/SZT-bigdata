package cn.java666.etlspringboot.controller;

import cn.java666.etlspringboot.service.RedisService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/redis")
@Tag(name = "Redis 查询", description = "深圳通 Redis 数据查询 API")
public class RedisController {

    private final RedisService redisService;

    public RedisController(RedisService redisService) {
        this.redisService = redisService;
    }

    @GetMapping("/{key}")
    @Operation(summary = "查询 Redis", description = "根据 key 查询 Redis 中的深圳通数据")
    public String get(@PathVariable String key) {
        return redisService.get(key);
    }
}
