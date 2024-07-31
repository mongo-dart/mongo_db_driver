import 'abstract/topology.dart';

class SharderdCluster extends Topology {
  SharderdCluster(super.mongoClient, super.hostsSeedList,
      {super.detectedServers, TopologyType? topologyType})
      : super.protected(type: topologyType ?? TopologyType.sharded);
}
