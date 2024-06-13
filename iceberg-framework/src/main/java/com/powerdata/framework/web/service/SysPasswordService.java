package com.powerdata.framework.web.service;

import com.powerdata.common.constant.CacheConstants;
import com.powerdata.common.constant.Constants;
import com.powerdata.common.core.domain.entity.SysUser;
import com.powerdata.common.exception.user.UserPasswordNotMatchException;
import com.powerdata.common.exception.user.UserPasswordRetryLimitExceedException;
import com.powerdata.common.utils.CacheUtils;
import com.powerdata.common.utils.MessageUtils;
import com.powerdata.common.utils.SecurityUtils;
import com.powerdata.framework.manager.AsyncManager;
import com.powerdata.framework.manager.factory.AsyncFactory;
import com.powerdata.framework.security.context.AuthenticationContextHolder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.Cache;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Component;

/**
 * 登录密码方法
 * 
 * @author
 */
@Component
public class SysPasswordService
{
    @Value(value = "${user.password.maxRetryCount}")
    private int maxRetryCount;

    @Value(value = "${user.password.lockTime}")
    private int lockTime;

    /**
     * 登录账户密码错误次数缓存键名
     *
     * @return 缓存Cache
     */
    private Cache getCache()
    {
        return CacheUtils.getCache(CacheConstants.PWD_ERR_CNT_KEY);
    }

    public void validate(SysUser user)
    {
        Authentication usernamePasswordAuthenticationToken = AuthenticationContextHolder.getContext();
        String username = usernamePasswordAuthenticationToken.getName();
        String password = usernamePasswordAuthenticationToken.getCredentials().toString();
        Integer retryCount = getCache().get(username, Integer.class);
        if (retryCount == null)
        {
            retryCount = 0;
        }
        if (retryCount >= Integer.valueOf(maxRetryCount).intValue())
        {
            AsyncManager.me().execute(AsyncFactory.recordLogininfor(username, Constants.LOGIN_FAIL,
                    MessageUtils.message("user.password.retry.limit.exceed", maxRetryCount, lockTime)));
            throw new UserPasswordRetryLimitExceedException(maxRetryCount, lockTime);
        }
        if (!matches(user, password))
        {
            retryCount = retryCount + 1;
            AsyncManager.me().execute(AsyncFactory.recordLogininfor(username, Constants.LOGIN_FAIL,
                    MessageUtils.message("user.password.retry.limit.count", retryCount)));
            getCache().put(username, retryCount);
            throw new UserPasswordNotMatchException();
        }
        else
        {
            clearLoginRecordCache(username);
        }
    }

    public boolean matches(SysUser user, String rawPassword)
    {
        return SecurityUtils.matchesPassword(rawPassword, user.getPassword());
    }

    public void clearLoginRecordCache(String loginName)
    {
        getCache().evictIfPresent(loginName);
    }
}
