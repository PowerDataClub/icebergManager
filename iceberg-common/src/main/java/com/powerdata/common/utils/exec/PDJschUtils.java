package com.powerdata.common.utils.exec;

import cn.hutool.extra.ssh.ChannelType;
import cn.hutool.extra.ssh.JschUtil;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class PDJschUtils {
    protected final Logger logger = LoggerFactory.getLogger(PDJschUtils.class);

//    private String ip;
//    private String port;
//    private String user;
//    private String passwd;
    private int timeout = 60 * 60 * 1000;

    private Session session = null;
    private ChannelExec channelExec = null;
    private BufferedReader inputStreamRead = null;
    private BufferedReader errInputStreamRead = null;

    private PDJschUtils(String ip, int port, String userName, String passwd) throws Exception {
        session = JschUtil.createSession(ip,port,userName,passwd);
        session.setTimeout(timeout);
        session.setConfig("userauth.gssapi-with-mic", "no");
        session.setConfig("StrictHostKeyChecking", "no");
        session.connect();
        channelExec = (ChannelExec) JschUtil.createChannel(session, ChannelType.EXEC);
    }

    public static PDJschUtils build(String ip, int port, String userName, String passwd) throws Exception {
        return new PDJschUtils(ip,port,userName,passwd);
    }

    public Map<String, String> exec(String cmd) {
        StringBuilder runLog = new StringBuilder();
        StringBuilder errLog = new StringBuilder();
        Map<String, String> resultMap = new HashMap<>();

        try {
            channelExec.setCommand(cmd);
            channelExec.connect();
            inputStreamRead = new BufferedReader(new InputStreamReader(channelExec.getInputStream()));
            errInputStreamRead = new BufferedReader(new InputStreamReader(channelExec.getErrStream()));

            String line = null;
            while ((line = inputStreamRead.readLine()) != null) runLog.append(line).append("\n");

            String errLine = null;
            while ((errLine = errInputStreamRead.readLine()) != null) errLog.append(errLine).append("\n");

            int resultKey = channelExec.getExitStatus();
            resultMap.put("resultKey", resultKey + "");

            logger.info("执行命令[{}],exitStatus=[{}],openChannel.isClosed=[{}]", cmd, resultKey, channelExec.isClosed());
//            logger.info("命令执行完成，执行日志如下：\n {}", runLog.toString());
//            logger.warn("命令执行完成，执行错误、告警日志如下：\n {}", errLog.toString());
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("执行命令[{}]异常，异常信息：", cmd, e);
            resultMap.put("error", e.getMessage());
        }
        resultMap.put("runLog", runLog.toString());
        resultMap.put("errLog", errLog.toString());
        return resultMap;
    }

    public void close(){
        JschUtil.close(channelExec);
        JschUtil.close(session);
    }


}
