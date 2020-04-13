package cn.java666.etlspringboot;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class EtlSApp {
	static {
		// es bug
		// System.setProperty("es.set.netty.runtime.available.processors", "false");
	}
	
	public static void main(String[] args) {
		SpringApplication.run(EtlSApp.class, args);
	}
}
