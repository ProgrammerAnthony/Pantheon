package com.pantheon.server.config;

import com.pantheon.common.ObjectUtils;
import com.pantheon.server.ServerBootstrap;
import com.netflix.config.ConfigurationManager;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.config.DynamicStringProperty;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author Anthony
 * @create 2021/11/17
 * @desc A default implementation of Pantheon Server configuration as required by {@link PantheonServerConfig}.
 * <p>
 * The information required for configuring pantheon server is provided in a
 * configuration file.The configuration file is searched for in the classpath
 * with the name specified by the property <em>pantheon.server.props</em> and with
 * the suffix <em>.properties</em>. If the property is not specified,
 * <em>pantheon-server.properties</em> is assumed as the default.The properties
 * that are looked up uses the <em>namespace</em> passed on to this class.
 * </p>
 *
 * <p>
 * If the <em>pantheon.environment</em> property is specified, additionally
 * <em>pantheon-server-<pantheon.environment>.properties</em> is loaded in addition
 * to <em>pantheon-server.properties</em>.
 * </p>
 * <p>
 * todo verify how to add config in application。properties
 **/
public class ArchaiusPantheonServerConfig implements PantheonServerConfig {
    private static final Logger logger = LoggerFactory.getLogger(ServerBootstrap.class);
    private static final String TEST = "test";
    private static final String PANTHEON_ENVIRONMENT = "pantheon.environment";
    private static final String ARCHAIUS_DEPLOYMENT_ENVIRONMENT = "archaius.deployment.environment";
    private static final DynamicPropertyFactory configInstance = com.netflix.config.DynamicPropertyFactory
            .getInstance();
    private static final DynamicStringProperty PANTHEON_PROPS_FILE = DynamicPropertyFactory
            .getInstance().getStringProperty("pantheon.server.props",
                    "pantheon-server");
    protected String namespace = "pantheon.";


    final String CONFIG_KEY_NODE_ID = namespace + "nodeId";
    final String CONFIG_KEY_NODE_IP = namespace + "nodeIp";
    final String CONFIG_KEY_INTERN_TCP_PORT = namespace + "nodeInternTcpPort";
    final String CONFIG_KEY_NODE_CLIENT_HTTP_PORT = namespace + "nodeClientHttpPort";
    final String CONFIG_KEY_CLIENT_TCP_PORT = namespace + "nodeClientTcpPort";
    final String CONFIG_KEY_CONTROLLER_CANDIDATE = namespace + "isControllerCandidate";
    final String CONFIG_KEY_DATA_DIR = namespace + "dataDir";
    final String CONFIG_KEY_CLUSTER_NODE_COUNT = namespace + "clusterNodeCount";
    final String CONFIG_KEY_CONTROLLER_CANDIDATE_SERVERS = namespace + "controllerCandidateServers";
    final String CONFIG_KEY_HEART_CHECK_INTERVAL = namespace + "heartbeatCheckInterval";
    final String CONFIG_KEY_HEART_TIMEOUT_PERIOD = namespace + "heartbeatTimeoutPeriod";


    public static final Integer DEFAULT_HEARTBEAT_CHECK_INTERVAL = 3;
    public static final Integer DEFAULT_HEARTBEAT_TIMEOUT_PERIOD = 5;



    public ArchaiusPantheonServerConfig() {
        initConfig();
        validateConfig();
    }

    public ArchaiusPantheonServerConfig(String namespace) {
        this.namespace = namespace;
        initConfig();
        validateConfig();
    }

    private void initConfig() {
        String env = ConfigurationManager.getConfigInstance().getString(
                PANTHEON_ENVIRONMENT, TEST);
        ConfigurationManager.getConfigInstance().setProperty(
                ARCHAIUS_DEPLOYMENT_ENVIRONMENT, env);
        String propsFile = PANTHEON_PROPS_FILE.get();
        try {
            //load PANTHEON_PROPS_FILE after application.properties
            ConfigurationManager
                    .loadCascadedPropertiesFromResources(propsFile);
        } catch (IOException e) {
            logger.warn(
                    "Cannot find the properties specified : {}. This may be okay if there are other environment "
                            + "specific properties or the configuration is installed with a different mechanism.",
                    propsFile);
        }
    }

    public void validateConfig() {
        validateNodeId(getNodeId());
        validateNodeIp(getNodeIp());
        validateNodeInternTcpPort(getNodeInternTcpPort());
        validateNodeClientHttpPort(getNodeClientHttpPort());
        validateNodeClientTcpPort(getNodeClientTcpPort());
        validateIsControllerCandidate(isControllerCandidate());
        validateClusterNodeCount(getClusterNodeCount());
        validateDataDir(getDataDir());
        validateControllerCandidateServers(getControllerCandidateServers());
    }

    /**
     * 校验节点id参数
     *
     * @param nodeId
     * @return
     */
    private boolean validateNodeId(Integer nodeId) {
        if (ObjectUtils.isEmpty(nodeId)) {
            throw new IllegalArgumentException("node.id cannot be empty！！！");
        }
        return true;
    }


    /**
     * validate ip address
     *
     * @param nodeIp
     * @return
     * @throws IllegalArgumentException
     */
    private Boolean validateNodeIp(String nodeIp) throws IllegalArgumentException {
        if (StringUtils.isEmpty(nodeIp)) {
            throw new IllegalArgumentException("node.ip cannot be empty！！！");
        }

        final String regex = "(\\d+\\.\\d+\\.\\d+\\.\\d+)";
        Boolean isMatch = Pattern.matches(regex, nodeIp);
        if (!isMatch) {
            throw new IllegalArgumentException("node.ip format is incorrect！！！");
        }

        return true;
    }

    /**
     * validate the tcp port to communicate with intern
     *
     * @return
     */
    private boolean validateNodeInternTcpPort(Integer nodeInternTcpPort) {
        if (ObjectUtils.isEmpty(nodeInternTcpPort)) {
            throw new IllegalArgumentException("node.intern.tcp.port cannot be empty！！！");
        }

        return true;
    }

    /**
     * validate the port to communicate with client
     *
     * @return
     */
    private boolean validateNodeClientHttpPort(Integer nodeClientHttpPort) {
        if (ObjectUtils.isEmpty(nodeClientHttpPort)) {
            throw new IllegalArgumentException("node.client.http.port cannot be empty！！！");
        }

        return true;
    }

    /**
     * validate the tcp port to communicate with client
     *
     * @return
     */
    private boolean validateNodeClientTcpPort(Integer nodeClientTcpPort) {
        if (ObjectUtils.isEmpty(nodeClientTcpPort)) {
            throw new IllegalArgumentException("node.client.tcp.port cannot be empty！！！");
        }

        return true;
    }

    /**
     * whether controller candidate
     *
     * @param isControllerCandidate
     * @return
     */
    private boolean validateIsControllerCandidate(Boolean isControllerCandidate) {
        if (ObjectUtils.isEmpty(isControllerCandidate)) {
            throw new IllegalArgumentException("is.controller.candidate cannot be empty ！！！");
        }
        return true;
    }


    /**
     * validate the cluster node count, for controller node，cluster node count cannot be empty
     *
     * @param clusterNodesCount
     * @return
     */
    private boolean validateClusterNodeCount(Integer clusterNodesCount) {
        if (isControllerCandidate() && ObjectUtils.isEmpty(clusterNodesCount)) {
            throw new IllegalArgumentException("for controller node，clusterNodeCount cannot be empty！！！");
        }

        return true;
    }

    /**
     * validate data directory for saving
     *
     * @param dataDir
     * @return
     */
    private Boolean validateDataDir(String dataDir) {
        if (StringUtils.isEmpty(dataDir)) {
            throw new IllegalArgumentException("data.dir cannot be empty......");
        }
        return true;
    }


    private Boolean validateControllerCandidateServers(String controllerCandidateServers) throws IllegalArgumentException {
        if (StringUtils.isEmpty(controllerCandidateServers)) {
            throw new IllegalArgumentException("controller.candidate.servers cannot be empty！！！");
        }

        String[] controllerCandidateServersSplited = controllerCandidateServers.split(",");

        final String regex = "(\\d+\\.\\d+\\.\\d+\\.\\d+):(\\d+)";

        for (String controllerCandidateServer : controllerCandidateServersSplited) {
            Boolean isMatch = Pattern.matches(regex, controllerCandidateServer);
            if (!isMatch) {
                throw new IllegalArgumentException("controller.candidate.servers format is incorrect！！！");
            }
        }

        return true;
    }

    @Override
    public String getDataDir() {
        String dataDir = configInstance.getStringProperty(CONFIG_KEY_DATA_DIR, "/").get();
        if (null != dataDir) {
            return dataDir.trim();
        } else {
            return null;
        }
    }


    @Override
    public Integer getNodeId() {
        return configInstance.getIntProperty(
                CONFIG_KEY_NODE_ID, 0).get();
    }

    @Override
    public Integer getHeartBeatCheckInterval() {
        return configInstance.getIntProperty(
                CONFIG_KEY_HEART_CHECK_INTERVAL, DEFAULT_HEARTBEAT_CHECK_INTERVAL).get();
    }

    //todo get timeout interval
    public Integer getHeartbeatTimeoutPeriod() {
        return configInstance.getIntProperty(
                CONFIG_KEY_HEART_CHECK_INTERVAL, DEFAULT_HEARTBEAT_CHECK_INTERVAL).get();
    }


    @Override
    public String getNodeIp() {
        String nodeIp = configInstance.getStringProperty(CONFIG_KEY_NODE_IP, null).get();
        if (null != nodeIp) {
            return nodeIp.trim();
        } else {
            return null;
        }
    }

    @Override
    public Integer getNodeInternTcpPort() {
        return configInstance.getIntProperty(
                CONFIG_KEY_INTERN_TCP_PORT, 0).get();
    }

    @Override
    public Integer getNodeClientTcpPort() {
        return configInstance.getIntProperty(
                CONFIG_KEY_CLIENT_TCP_PORT, 0).get();
    }

    @Override
    public Boolean isControllerCandidate() {
        return configInstance.getBooleanProperty(CONFIG_KEY_CONTROLLER_CANDIDATE, false).get();
    }

    @Override
    public Integer getClusterNodeCount() {
        return configInstance.getIntProperty(
                CONFIG_KEY_CLUSTER_NODE_COUNT, 0).get();
    }

    @Override
    public Integer getNodeClientHttpPort() {
        return configInstance.getIntProperty(
                CONFIG_KEY_NODE_CLIENT_HTTP_PORT, 0).get();
    }

    @Override
    public String getControllerCandidateServers() {
        return configInstance.getStringProperty(CONFIG_KEY_CONTROLLER_CANDIDATE_SERVERS, null).get();
    }


}


