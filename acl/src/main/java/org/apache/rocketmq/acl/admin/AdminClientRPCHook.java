package org.apache.rocketmq.acl.admin;
import static org.apache.rocketmq.acl.common.SessionCredentials.ACCESS_KEY;
import static org.apache.rocketmq.acl.common.SessionCredentials.SIGNATURE;

import java.lang.reflect.Field;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.rocketmq.acl.common.AclUtils;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
/**
 * 管理员客户端rpc钩子，用于跟NameServer交互
 * 
 * @author yongfeigao
 * @date 2019年12月16日
 */
public class AdminClientRPCHook implements RPCHook {
    protected ConcurrentHashMap<Class<? extends CommandCustomHeader>, Field[]> fieldCache = 
            new ConcurrentHashMap<Class<? extends CommandCustomHeader>, Field[]>();

    private AdminResourceConfig adminResourceConfig;
    
    public AdminClientRPCHook(AdminResourceConfig adminResourceConfig) {
        this.adminResourceConfig = adminResourceConfig;
    }

    @Override
    public void doBeforeRequest(String remoteAddr, RemotingCommand request) {
        byte[] total = AclUtils.combineRequestContent(request,
                parseRequestContent(request, adminResourceConfig.getAccessKey()));
        String signature = AclUtils.calSignature(total, adminResourceConfig.getSecretKey());
        request.addExtField(ACCESS_KEY, adminResourceConfig.getAccessKey());
        request.addExtField(SIGNATURE, signature);
    }

    @Override
    public void doAfterResponse(String remoteAddr, RemotingCommand request, RemotingCommand response) {

    }

    protected SortedMap<String, String> parseRequestContent(RemotingCommand request, String ak) {
        CommandCustomHeader header = request.readCustomHeader();
        // Sort property
        SortedMap<String, String> map = new TreeMap<String, String>();
        map.put(ACCESS_KEY, ak);
        try {
            // Add header properties
            if (null != header) {
                Field[] fields = fieldCache.get(header.getClass());
                if (null == fields) {
                    fields = header.getClass().getDeclaredFields();
                    for (Field field : fields) {
                        field.setAccessible(true);
                    }
                    Field[] tmp = fieldCache.putIfAbsent(header.getClass(), fields);
                    if (null != tmp) {
                        fields = tmp;
                    }
                }

                for (Field field : fields) {
                    Object value = field.get(header);
                    if (null != value && !field.isSynthetic()) {
                        map.put(field.getName(), value.toString());
                    }
                }
            }
            return map;
        } catch (Exception e) {
            throw new RuntimeException("incompatible exception.", e);
        }
    }
}
