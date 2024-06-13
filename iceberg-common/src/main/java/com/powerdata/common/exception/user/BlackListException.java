package com.powerdata.common.exception.user;

public class BlackListException extends UserException {
    private static final long serialVersionUID = 1L;

    public BlackListException()
    {
        super("login.blocked", null);
    }
}
