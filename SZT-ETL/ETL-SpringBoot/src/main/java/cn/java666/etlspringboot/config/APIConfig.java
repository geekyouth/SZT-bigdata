package cn.java666.etlspringboot.config;

import com.github.xiaoymin.knife4j.spring.annotations.EnableKnife4j;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import springfox.bean.validators.configuration.BeanValidatorPluginsConfiguration;
import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

/**
 * @author Geek
 * @date 2020-04-13 20:35:26
 *
 * knife4j 前身为 swagger-bootstrap-ui
 */

@Slf4j
@Configuration
@EnableSwagger2
@EnableKnife4j
@Import(BeanValidatorPluginsConfiguration.class)
public class APIConfig {
	@Value("${server.port}")
	private int port;
	
	@Value("${server.address}")
	private String url;
	
	private String docPath;
	
	@Bean(value = "defaultApi2")
	public Docket defaultApi2() {
		docPath = "http://" + url + ":" + port + "/doc.html";
		log.warn("API 文档地址={} --------------------", docPath);
		
		return new Docket(DocumentationType.SWAGGER_2)
			.apiInfo(apiInfo())
			//分组名称
			.groupName("ETL-SpringBoot")
			.select()
			//这里指定Controller扫描包路径
			.apis(RequestHandlerSelectors.basePackage("cn.java666.etlspringboot.controller"))
			.paths(PathSelectors.any())
			.build();
	}
	
	private ApiInfo apiInfo() {
		return new ApiInfoBuilder()
			.title("ETL-SpringBoot")
			.description(" RESTful API 调试 ")
			.termsOfServiceUrl(docPath)
			.contact("forsupergeeker@gmail.com")
			.version("0.1")
			.build();
	}
}
