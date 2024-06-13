package com.powerdata.web.controller.monitor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.springframework.cache.Cache.ValueWrapper;
import org.springframework.data.redis.cache.RedisCacheManager;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.powerdata.common.constant.CacheConstants;
import com.powerdata.common.core.domain.AjaxResult;
import com.powerdata.common.core.text.Convert;
import com.powerdata.common.utils.CacheUtils;
import com.powerdata.common.utils.StringUtils;
import com.powerdata.system.domain.SysCache;

/**
 * 缓存监控
 * 
 * @author
 */
@RestController
@RequestMapping("/monitor/cache")
public class CacheController
{
    private static String tmpCacheName = "";
    
    private final static List<SysCache> caches = new ArrayList<SysCache>();
    {
        caches.add(new SysCache(CacheConstants.LOGIN_TOKEN_KEY, "用户信息"));
        caches.add(new SysCache(CacheConstants.SYS_CONFIG_KEY, "配置信息"));
        caches.add(new SysCache(CacheConstants.SYS_DICT_KEY, "数据字典"));
        caches.add(new SysCache(CacheConstants.CAPTCHA_CODE_KEY, "验证码"));
        caches.add(new SysCache(CacheConstants.REPEAT_SUBMIT_KEY, "防重提交"));
        caches.add(new SysCache(CacheConstants.RATE_LIMIT_KEY, "限流处理"));
        caches.add(new SysCache(CacheConstants.PWD_ERR_CNT_KEY, "密码错误次数"));
    }

    @PreAuthorize("@ss.hasPermi('monitor:cache:list')")
    @GetMapping()
    public AjaxResult getInfo() throws Exception
    {
        Map<String, Object> result = new HashMap<>(3);
        if (CacheUtils.getCacheManager() instanceof RedisCacheManager)
        {
            Properties info = (Properties) CacheUtils.getRedisTemplate().execute((RedisCallback<Object>) connection -> connection.info());
            Properties commandStats = (Properties) CacheUtils.getRedisTemplate().execute((RedisCallback<Object>) connection -> connection.info("commandstats"));
            Object dbSize = CacheUtils.getRedisTemplate().execute((RedisCallback<Object>) connection -> connection.dbSize());
            result.put("info", info);
            result.put("dbSize", dbSize);
            List<Map<String, String>> pieList = new ArrayList<>();
            commandStats.stringPropertyNames().forEach(key -> {
                Map<String, String> data = new HashMap<>(2);
                String property = commandStats.getProperty(key);
                data.put("name", StringUtils.removeStart(key, "cmdstat_"));
                data.put("value", StringUtils.substringBetween(property, "calls=", ",usec"));
                pieList.add(data);
            });
            result.put("commandStats", pieList);
        }
        return AjaxResult.success(result);
    }

    @PreAuthorize("@ss.hasPermi('monitor:cache:list')")
    @GetMapping("/getNames")
    public AjaxResult cache()
    {
        return AjaxResult.success(caches);
    }

    @PreAuthorize("@ss.hasPermi('monitor:cache:list')")
    @GetMapping("/getKeys/{cacheName}")
    public AjaxResult getCacheKeys(@PathVariable String cacheName)
    {
        tmpCacheName = cacheName;
        Set<String> keyset = CacheUtils.getkeys(cacheName);
        return AjaxResult.success(keyset);
    }

    @PreAuthorize("@ss.hasPermi('monitor:cache:list')")
    @GetMapping("/getValue/{cacheName}/{cacheKey}")
    public AjaxResult getCacheValue(@PathVariable String cacheName, @PathVariable String cacheKey)
    {
        ValueWrapper valueWrapper = CacheUtils.get(cacheName, cacheKey);
        SysCache sysCache = new SysCache();
        sysCache.setCacheName(cacheName);
        sysCache.setCacheKey(cacheKey);
        if (StringUtils.isNotNull(valueWrapper))
        {
            sysCache.setCacheValue(Convert.toStr(valueWrapper.get(), ""));
        }
        return AjaxResult.success(sysCache);
    }

    @PreAuthorize("@ss.hasPermi('monitor:cache:list')")
    @DeleteMapping("/clearCacheName/{cacheName}")
    public AjaxResult clearCacheName(@PathVariable String cacheName)
    {
        CacheUtils.clear(cacheName);
        return AjaxResult.success();
    }

    @PreAuthorize("@ss.hasPermi('monitor:cache:list')")
    @DeleteMapping("/clearCacheKey/{cacheKey}")
    public AjaxResult clearCacheKey(@PathVariable String cacheKey)
    {
        CacheUtils.removeIfPresent(tmpCacheName, cacheKey);
        return AjaxResult.success();
    }

    @PreAuthorize("@ss.hasPermi('monitor:cache:list')")
    @DeleteMapping("/clearCacheAll")
    public AjaxResult clearCacheAll()
    {
        for (String cacheName : CacheUtils.getCacheManager().getCacheNames())
        {
            CacheUtils.clear(cacheName);
        }
        return AjaxResult.success();
    }
}
