package org.apache.rocketmq.acl.admin;

import static org.apache.rocketmq.acl.common.SessionCredentials.ACCESS_KEY;
import static org.apache.rocketmq.acl.common.SessionCredentials.SIGNATURE;

import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import com.google.protobuf.GeneratedMessageV3;
import org.apache.rocketmq.acl.AccessResource;
import org.apache.rocketmq.acl.AccessValidator;
import org.apache.rocketmq.acl.common.AclException;
import org.apache.rocketmq.acl.common.AclUtils;
import org.apache.rocketmq.acl.common.AuthenticationHeader;
import org.apache.rocketmq.common.AclConfig;
import org.apache.rocketmq.common.PlainAccessConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.DataVersion;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * 管理员校验
 * 
 * @author yongfeigao
 * @date 2019年12月16日
 */
public class AdminAccessValidator implements AccessValidator {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    private AdminResourceConfig adminResourceConfig;

    public AdminAccessValidator(AdminResourceConfig adminResourceConfig) {
        this.adminResourceConfig = adminResourceConfig;
    }

    @Override
    public AccessResource parse(RemotingCommand request, String remoteAddr) {
        AdminAccessResource adminAccessResource = new AdminAccessResource();
        adminAccessResource.setRequestCode(request.getCode());
        adminAccessResource.setRemoteAddress(remoteAddr);
        if (request.getExtFields() == null) {
            return adminAccessResource;
        }
        adminAccessResource.setAccessKey(request.getExtFields().get(ACCESS_KEY));
        adminAccessResource.setSignature(request.getExtFields().get(SIGNATURE));

        // Content
        SortedMap<String, String> map = new TreeMap<String, String>();
        for (Map.Entry<String, String> entry : request.getExtFields().entrySet()) {
            if (!SIGNATURE.equals(entry.getKey())) {
                map.put(entry.getKey(), entry.getValue());
            }
        }
        adminAccessResource.setContent(AclUtils.combineRequestContent(request, map));
        return adminAccessResource;
    }

    @Override
    public AccessResource parse(GeneratedMessageV3 messageV3, AuthenticationHeader header) {
        return null;
    }

    @Override
    public void validate(AccessResource accessResource) {
        AdminAccessResource adminResource = (AdminAccessResource) accessResource;
        if (!adminResourceConfig.isAdminRequest(adminResource.getRequestCode())) {
            return;
        }

        if (adminResource.getAccessKey() == null) {
            log.warn("remoteAddr:{} requestCode:{} accessKey is null", adminResource.getRemoteAddress(),
                    adminResource.getRequestCode());
            throw new AclException(String.format("No accessKey is configured"));
        }

        if (!adminResource.getAccessKey().equals(adminResourceConfig.getAccessKey())) {
            log.warn("remoteAddr:{} requestCode:{} accessKey:{} no config", adminResource.getRemoteAddress(),
                    adminResource.getRequestCode(), adminResource.getAccessKey());
            throw new AclException(String.format("No acl config for %s", adminResource.getAccessKey()));
        }

        // Check the signature
        String signature = AclUtils.calSignature(adminResource.getContent(), adminResourceConfig.getSecretKey());
        if (!signature.equals(adminResource.getSignature())) {
            log.warn("remoteAddr:{} requestCode:{} accessKey:{}, signature:{} not right",
                    adminResource.getRemoteAddress(),
                    adminResource.getRequestCode(), adminResource.getAccessKey(), signature);
            throw new AclException(
                    String.format("Check signature failed for accessKey=%s", adminResource.getAccessKey()));
        }
    }

    @Override
    public boolean updateAccessConfig(PlainAccessConfig plainAccessConfig) {
        return false;
    }

    @Override
    public boolean deleteAccessConfig(String accesskey) {
        return false;
    }

    @Override
    public String getAclConfigVersion() {
        return null;
    }

    @Override
    public boolean updateGlobalWhiteAddrsConfig(List<String> globalWhiteAddrsList) {
        return false;
    }

    @Override
    public boolean updateGlobalWhiteAddrsConfig(List<String> globalWhiteAddrsList, String aclFileFullPath) {
        return false;
    }

    @Override
    public AclConfig getAllAclConfig() {
        return null;
    }

    @Override
    public Map<String, DataVersion> getAllAclConfigVersion() {
        return null;
    }
}
