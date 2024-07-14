import '../client/client_exp.dart';
import 'abstract/topology.dart';
import 'replica_set.dart';
import 'sharded_cluster.dart';
import 'standalone.dart';

/// This is a class used uniquely to discover which is the
/// topology of our connection.
/// It tries to connect on each seed server until it is able to
/// discover the topology.
/// Once that it is done it can build the correct object.
/// Here the connection is made only on one server.
/// The correct topology object will have to complete all the connections.
class Discover extends Topology {
  Discover(super.mongoClient, super.hostsSeedList) : super.protected() {
    type = TopologyType.unknown;
  }

  Future<Topology> getEffectiveTopology() async {
    Topology topology;
    if (servers.first.isStandalone) {
      topology =
          Standalone(mongoClient, hostsSeedList, detectedServers: servers);
      topology.primary = servers.first;
    } else if (servers.first.isReplicaSet) {
      topology =
          ReplicaSet(mongoClient, hostsSeedList, detectedServers: servers);
      await topology.updateServersStatus();
    } else if (servers.first.isShardedCluster) {
      // Todo
      topology =
          SharderdCluster(mongoClient, hostsSeedList, detectedServers: servers);
    } else {
      throw MongoDartError('Unknown topology type');
    }

    return topology;
  }
}
