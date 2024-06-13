package com.powerdata;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
/**
 * 启动程序
 * 
 * @author ruoyi
 */
@SpringBootApplication(exclude = { DataSourceAutoConfiguration.class })
@EnableWebMvc
@EnableCaching
@EnableScheduling
public class IcebergManagerApplication
{
    public static void main(String[] args)
    {
        // System.setProperty("spring.devtools.restart.enabled", "false");
        SpringApplication.run(IcebergManagerApplication.class, args);
        System.out.println("////////////////////////////////////////////////////////////////////");
        System.out.println("//                IcebergManager@POWERDATA启动成功                  //");
        System.out.println("////////////////////////////////////////////////////////////////////");
    }
}
