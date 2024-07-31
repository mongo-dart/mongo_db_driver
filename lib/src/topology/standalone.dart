import 'abstract/topology.dart';

class Standalone extends Topology {
  Standalone(super.mongoClient, super.hostsSeedList, {super.detectedServers})
      : super.protected(type: TopologyType.single);

  @override
  Future<void> addServersFromSeedList() async {
    await super.addServersFromSeedList();
    primary = servers.isNotEmpty ? servers.first : null;
  }
}
