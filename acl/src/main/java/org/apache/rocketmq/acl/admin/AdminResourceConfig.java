package org.apache.rocketmq.acl.admin;

import java.util.Set;

import org.apache.rocketmq.acl.common.AclException;
import org.apache.rocketmq.acl.common.AclUtils;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.srvutil.FileWatchService;

import com.alibaba.fastjson.JSONObject;
/**
 * 管理员配置加载
 * 
 * @author yongfeigao
 * @date 2019年12月16日
 */
public class AdminResourceConfig {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    private String fileHome = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY, System.getenv(MixAll.ROCKETMQ_HOME_ENV));

    private String file;
    
    private AdminConfig adminConfig;
    
    public static AdminResourceConfig getBrokerAdminResourceConfig() {
        return new AdminResourceConfig("admin_broker_acl.yml");
    }
    
    public static AdminResourceConfig getNameServerAdminResourceConfig() {
        return new AdminResourceConfig("admin_ns_acl.yml");
    }

    public AdminResourceConfig(String fileName) {
        this.file = fileHome + "/conf/" + fileName;;
        load();
        watch();
    }
    
    /**
     * 加载配置
     */
    public void load() {
        JSONObject jsonData = AclUtils.getYamlDataObject(file, JSONObject.class);
        AdminConfig adminConfig = jsonData.toJavaObject(AdminConfig.class);
        if (adminConfig == null) {
            throw new AclException(String.format("%s file is no data", file));
        }
        log.info("Broker admin acl conf data is:{}", adminConfig);
        if(adminConfig.getAccessKey() == null) {
            throw new AclException(String.format("%s file accessKey is null", file));
        }
        if(adminConfig.getSecretKey() == null) {
            throw new AclException(String.format("%s file secretKey is null", file));
        }
        if(adminConfig.getAdminRequestCode() == null) {
            throw new AclException(String.format("%s file adminRequestCode is null", file));
        }
        this.adminConfig = adminConfig;
    }

    /**
     * 监控文件变动
     */
    private void watch() {
        try {
            FileWatchService fileWatchService = new FileWatchService(new String[] {file},
                    new FileWatchService.Listener() {
                        @Override
                        public void onChanged(String path) {
                            log.info("The admin acl yml changed, reload the context");
                            load();
                        }
                    });
            fileWatchService.start();
            log.info("Succeed to start AclWatcherService");
        } catch (Exception e) {
            log.error("Failed to start AclWatcherService", e);
        }
    }

    static class AdminConfig {

        private String accessKey;

        private String secretKey;
        
        private Set<Integer> adminRequestCode;

        public String getAccessKey() {
            return accessKey;
        }

        public void setAccessKey(String accessKey) {
            this.accessKey = accessKey;
        }

        public String getSecretKey() {
            return secretKey;
        }

        public void setSecretKey(String secretKey) {
            this.secretKey = secretKey;
        }

        public Set<Integer> getAdminRequestCode() {
            return adminRequestCode;
        }

        public void setAdminRequestCode(Set<Integer> adminRequestCode) {
            this.adminRequestCode = adminRequestCode;
        }

        @Override
        public String toString() {
            return "AdminConfig [accessKey=" + accessKey + ", secretKey=" + secretKey + ", adminRequestCode="
                    + adminRequestCode + "]";
        }
    }

    public String getAccessKey() {
        return adminConfig.getAccessKey();
    }

    public String getSecretKey() {
        return adminConfig.getSecretKey();
    }
    
    public boolean isAdminRequest(int requestCode) {
        return adminConfig.getAdminRequestCode().contains(requestCode);
    }
}
