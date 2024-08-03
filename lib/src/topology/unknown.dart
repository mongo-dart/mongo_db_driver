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
class Unknown extends Topology {
  Unknown(super.mongoClient, super.hostsSeedList, {TopologyType? topologyType})
      : super.protected(type: topologyType ?? TopologyType.unknown);

  /// If the TopologyType is **not** Single, the topology can contain zero or
  /// more servers. The state of topology containing zero servers is terminal
  /// (because servers can only be added if they are reported by a server
  /// already in the topology).
  /// A client SHOULD emit a warning if it is
  /// constructed with no seeds in the initial seed list. A client SHOULD
  /// emit a warning when, in the process of updating its topology description,
  /// it removes the last server from the topology.
  ///
  /// Whenever a client completes a hello or legacy hello call, it creates a new
  ///  ServerDescription with the proper [ServerType](#servertype).
  /// It replaces the server's previous description in
  /// TopologyDescription.servers with the new one.
  ///
  /// Apply the logic for [checking wire protocol compatibility](#checking-wire-protocol-compatibility)
  ///  to each ServerDescription in the topology. If any server's wire protocol
  /// version range does not overlap with the client's, the client updates the
  /// "compatible" and "compatibilityError" fields as described above for
  /// TopologyType Single. Otherwise "compatible" is set to true.
  ///
  /// It is possible for a multi-threaded client to receive a hello or legacy
  /// hello outcome from a server after the server has been removed from the
  /// TopologyDescription. For example, a monitor begins checking a server "A",
  ///  then a different monitor receives a response from the primary claiming
  /// that "A" has been removed from the replica set, so the client removes "A"
  /// from the TopologyDescription. Then, the check of server "A" completes.
  ///
  /// In all cases, the client MUST ignore hello or legacy hello outcomes from
  /// servers that are not in the TopologyDescription.
  ///
  /// The following subsections explain in detail what actions the client
  /// takes after replacing the ServerDescription.
  ///
  /// ##### TopologyType table
  ///
  /// The new ServerDescription's type is the vertical axis, and the current
  /// TopologyType is the horizontal. Where a ServerType and a TopologyType
  /// intersect, the table shows what action the client takes.
  ///
  /// "no-op" means, do nothing **after** replacing the server's old description
  /// with the new one.
  ///
  /// |                        | TopologyType Unknown                                                                            | TopologyType Sharded | TopologyType ReplicaSetNoPrimary                                                            | TopologyType ReplicaSetWithPrimary                              |
  /// | ---------------------- | ----------------------------------------------------------------------------------------------- | -------------------- | ------------------------------------------------------------------------------------------- | --------------------------------------------------------------- |
  /// | ServerType Unknown     | no-op                                                                                           | no-op                | no-op                                                                                       | [checkIfHasPrimary](#checkifhasprimary)                         |
  /// | ServerType Standalone  | [updateUnknownWithStandalone](#updateunknownwithstandalone)                                     | [remove](#remove)    | [remove](#remove)                                                                           | [remove](#remove) and [checkIfHasPrimary](#checkifhasprimary)   |
  /// | ServerType Mongos      | Set topology type to Sharded                                                                    | no-op                | [remove](#remove)                                                                           | [remove](#remove) and [checkIfHasPrimary](#checkifhasprimary)   |
  /// | ServerType RSPrimary   | Set topology type to ReplicaSetWithPrimary then [updateRSFromPrimary](#updatersfromprimary)     | [remove](#remove)    | Set topology type to ReplicaSetWithPrimary then [updateRSFromPrimary](#updatersfromprimary) | [updateRSFromPrimary](#updatersfromprimary)                     |
  /// | ServerType RSSecondary | Set topology type to ReplicaSetNoPrimary then [updateRSWithoutPrimary](#updaterswithoutprimary) | [remove](#remove)    | [updateRSWithoutPrimary](#updaterswithoutprimary)                                           | [updateRSWithPrimaryFromMember](#updaterswithprimaryfrommember) |
  /// | ServerType RSArbiter   | Set topology type to ReplicaSetNoPrimary then [updateRSWithoutPrimary](#updaterswithoutprimary) | [remove](#remove)    | [updateRSWithoutPrimary](#updaterswithoutprimary)                                           | [updateRSWithPrimaryFromMember](#updaterswithprimaryfrommember) |
  /// | ServerType RSOther     | Set topology type to ReplicaSetNoPrimary then [updateRSWithoutPrimary](#updaterswithoutprimary) | [remove](#remove)    | [updateRSWithoutPrimary](#updaterswithoutprimary)                                           | [updateRSWithPrimaryFromMember](#updaterswithprimaryfrommember) |
  /// | ServerType RSGhost     | no-op[^2]                                                                                       | [remove](#remove)    | no-op                                                                                       | [checkIfHasPrimary](#checkifhasprimary)                         |
  // https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.md#other-topologytypes
  // TODO
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
