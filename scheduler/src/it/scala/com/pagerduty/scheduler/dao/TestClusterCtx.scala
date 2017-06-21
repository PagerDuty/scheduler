package com.pagerduty.scheduler.dao

import com.netflix.astyanax.connectionpool.NodeDiscoveryType
import com.netflix.astyanax.connectionpool.impl.{CountingConnectionPoolMonitor, ConnectionPoolConfigurationImpl}
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl
import com.pagerduty.eris.ClusterCtx

class TestClusterCtx
    extends ClusterCtx(
      clusterName = "CassCluster",
      astyanaxConfig = new AstyanaxConfigurationImpl()
        .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE),
      connectionPoolConfig = new ConnectionPoolConfigurationImpl("CassConnectionPool")
        .setSeeds("localhost:9160")
        .setPort(9160),
      connectionPoolMonitor = new CountingConnectionPoolMonitor()
    )
