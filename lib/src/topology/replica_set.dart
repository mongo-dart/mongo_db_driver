import 'package:meta/meta.dart';
import 'package:mongo_db_driver/src/topology/server.dart';

import '../command/parameters/read_preference.dart';
import '../core/error/mongo_dart_error.dart';
import 'abstract/topology.dart';

class ReplicaSet extends Topology {
  ReplicaSet(super.mongoClient, super.hostsSeedList,
      {super.detectedServers, TopologyType? topologyType})
      : super.protected(type: topologyType ?? TopologyType.replicaSetNoPrimary);

  Set<Server> secondaries = <Server>{};

  @override
  bool get isReadOnly =>
      servers.every((element) => element.isConnected && element.isReadOnlyMode);

  @override
  Server getServer({ReadPreferenceMode? readPreferenceMode}) {
    var locReadPreferenceMode =
        readPreferenceMode ?? ReadPreferenceMode.primary;
    switch (locReadPreferenceMode) {
      case ReadPreferenceMode.primary:
        return primary != null
            ? primary!
            : throw MongoDartError('No primary detected');
      case ReadPreferenceMode.primaryPreferred:
        return primary != null && primary!.isConnected
            ? primary!
            : firstSecondary();
      case ReadPreferenceMode.secondary:
        return firstSecondary();
      case ReadPreferenceMode.secondaryPreferred:
        return firstSecondary(acceptAlsoPrimary: true);
      case ReadPreferenceMode.nearest:
        return nearest();
    }
  }

  @override
  Future<void> updateServersStatus() async {
    await super.updateServersStatus();
    updateServerClassification();
  }

  @override
  @protected
  Future<Set<Server>> addOtherServers(
      Server server, Set<Server> additionalServers) async {
    var addedServers = <Server>{};

    /// An RSGhost server has no hosts list nor setName. Therefore the client
    ///  MUST NOT attempt to use its hosts list nor check its setName.
    /// However, the client MUST keep the RSGhost member in its
    /// TopologyDescription, in case the client's only hope for staying
    ///  connected to the replica set is that this member will transition to a
    /// more useful state.
    // https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.md#rsghost-and-rsother
    if (server.serverType == ServerType.rsGhost) {
      return addedServers;
    }
    // TODO check if also passives and arbiters are needed
    if (server.hello?.hosts != null) {
      for (var url in server.hello!.hosts!) {
        if (servers.any((element) => element.url == url)) {
          continue;
        }
        if (additionalServers.any((element) => element.url == url)) {
          continue;
        }
        var serverConfig =
            await parseUri(Uri.parse('mongodb://$url'), mongoClientOptions);
        var server = Server(mongoClient, serverConfig,
            mongoClientOptions.connectionPoolSettings);
        addedServers.add(server);
      }
    }

    return addedServers;
  }

  /// Checks for found servers
  ///
  /// Logics:
  /// - In some circumstances, two nodes in a replica set may transiently believe
  /// that they are the primary, but at most, one of them will be able to
  /// complete writes with { w: "majority" } write concern. The node that
  /// can complete { w: "majority" } writes is the current primary, and
  /// the other node is a former primary that has not yet recognized its
  /// demotion, typically due to a network partition. When this occurs,
  /// clients that connect to the former primary may observe stale data
  /// despite having requested read preference primary, and new writes to the
  /// former primary will eventually roll back.
  updateServerClassification() {
    primary = null;
    secondaries.clear();
    for (Server server in servers) {
      if (server.isWritablePrimary) {
        // TODO what if two primaries?
        primary = server;
      } else {
        secondaries.add(server);
      }
    }
    if (primary != null) {
      if (secondaries.isNotEmpty) {
        type = TopologyType.replicaSetWithPrimary;
      } else {
        type = TopologyType.single;
      }
    } else {
      type = TopologyType.replicaSetNoPrimary;
    }
  }

  Server firstSecondary({bool? acceptAlsoPrimary}) {
    acceptAlsoPrimary ??= false;
    for (Server secondary in secondaries) {
      if (secondary.isConnected) {
        return secondary;
      }
    }
    return secondaries.isNotEmpty
        ? secondaries.first
        : (acceptAlsoPrimary && primary != null
            ? primary!
            : throw MongoDartError('No server detected'));
  }
}
