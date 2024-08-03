import 'package:bson/bson.dart';
import 'package:mongo_db_query/mongo_db_query.dart';

import '../../command/mixin/timing_result.dart';
import '../../topology/server.dart';
import '../../topology/topology_version.dart';
import '../error/mongo_dart_error.dart';

const writableServerTypes = <ServerType>{
  ServerType.rsPrimary,
  ServerType.standalone,
  ServerType.mongos,
  ServerType.loadBalancer
};

const dataBearingServerTypes = <ServerType>{
  ServerType.rsPrimary,
  ServerType.rsSecondary,
  ServerType.mongos,
  ServerType.standalone,
  ServerType.loadBalancer
};

typedef TagSet = Map<String, String>;

class ServerDescriptionOptions {
  /// An Error used for better reporting debugging
  MongoDartError? error;

  /// The average round trip time to ping this server (in ms)
  int? roundTripTime;

  /// The minimum round trip time to ping this server over the past 10 samples(in ms)
  int? minRoundTripTime;

  /// If the client is in load balancing mode.
  /// defaults to false
  bool loadBalanced = false;

  /// Comparison operator.
  @override
  bool operator ==(other) =>
      other is ServerDescriptionOptions &&
      error == other.error &&
      roundTripTime == other.roundTripTime &&
      minRoundTripTime == other.minRoundTripTime &&
      loadBalanced == other.loadBalanced;

  /// Hash Code.
  @override
  int get hashCode =>
      Object.hashAll([error, roundTripTime, minRoundTripTime, loadBalanced]);
}

/// The client's view of a single server, based on the most recent hello outcome.
class ServerDescription {
  /// Create a ServerDescription
  ServerDescription(this.address,
      {ServerDescriptionOptions? options,
      this.type = ServerType.unknown,
      Set<String>? hosts,
      Set<String>? passives,
      Set<String>? arbiters,
      TagSet? tags,
      //MongoDartError? error,
      this.topologyVersion,
      int? minWireVersion,
      int? maxWireVersion,
      //int? roundTripTime,
      //int? minRoundTripTime,
      //bool? loadBalancer,
      DateTime? lastUpdateTime,
      this.lastWrite,
      this.me,
      this.primary,
      this.setName,
      this.setVersion,
      this.electionId,
      this.logicalSessionTimeoutMinutes,
      this.operationTime,
      this.isCryptd,
      this.$clusterTime})
      : options = options ?? ServerDescriptionOptions(),
        hosts = hosts ?? const <String>{},
        passives = passives ?? const <String>{},
        arbiters = arbiters ?? const <String>{},
        tags = tags ?? const <String, String>{},
        minWireVersion = minWireVersion ?? 0,
        maxWireVersion = maxWireVersion ?? 0,
        lastUpdateTime = lastUpdateTime ?? DateTime(1970) {
    if (address.isEmpty) {
      throw MongoDartError(
          'ServerDescription must be provided with a non-empty address');
    }

    /*  options.error = error;
    options.roundTripTime = roundTripTime;
    options.minRoundTripTime = minRoundTripTime; */
  }

  final ServerDescriptionOptions options;
  String address;
  ServerType type;
  Set<String> hosts;
  Set<String> passives;
  Set<String> arbiters;
  TagSet tags;
  TopologyVersion? topologyVersion;
  int minWireVersion;
  int maxWireVersion;
  DateTime lastUpdateTime;
  MongoDocument? lastWrite;
  DateTime? operationTime;
  String? me;
  String? primary;
  String? setName;
  int? setVersion;
  ObjectId? electionId;
  int? logicalSessionTimeoutMinutes;

  /// boolean indicating if the server is a mongocryptd server. Default null.
  bool? isCryptd;

  // NOTE: does this belong here? It seems we should gossip the cluster
  //time at the CMAP level
  $ClusterTime? $clusterTime;

  List<String> get allHosts => [...hosts, ...arbiters, ...passives];

  /// Is this server available for reads
  bool get isReadable => type == ServerType.rsSecondary || isWritable;

  /// Is this server data bearing
  bool get isDataBearing => dataBearingServerTypes.contains(type);

  /// Is this server available for writes
  bool get isWritable => writableServerTypes.contains(type);

  String get host => address.split(':').first;

  int get port {
    var port = address.split(':').last;
    return int.tryParse(port) ?? 27017;
  }

  /// Determines if another `ServerDescription` is equal to this one per the
  ///  rules defined in this [spec](https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.md#serverdescription)
  @override
  bool operator ==(other) =>
      other is ServerDescription &&
      options.error == other.options.error &&
      type == other.type &&
      minWireVersion == other.minWireVersion &&
      maxWireVersion == other.maxWireVersion &&
      me == other.me &&
      setStrictEqual(hosts, other.hosts) &&
      setStrictEqual(passives, other.passives) &&
      setStrictEqual(arbiters, other.arbiters) &&
      tagsStrictEqual(tags, other.tags) &&
      setName == other.setName &&
      electionId == other.electionId &&
      setVersion == other.setVersion &&
      primary == other.primary &&
      logicalSessionTimeoutMinutes == other.logicalSessionTimeoutMinutes &&
      topologyVersion == other.topologyVersion &&
      isCryptd == other.isCryptd;

  /// Hash Code.
  /// Note, it is correct only after the first hello message (connection)
  /// Do not add to containers before that.
  @override
  int get hashCode => Object.hashAll([
        options.error,
        type,
        minWireVersion,
        maxWireVersion,
        me,
        host,
        passives,
        arbiters,
        tags,
        setName,
        electionId,
        setVersion,
        primary,
        logicalSessionTimeoutMinutes,
        topologyVersion,
        isCryptd
      ]);
}

bool tagsStrictEqual(TagSet? tags, TagSet? tags2) {
  if (tags == null && tags2 == null) {
    return true;
  }
  if (tags == null || tags2 == null) {
    return false;
  }
  var tagsKeys = tags.keys;
  var tags2Keys = tags2.keys;

  return (tagsKeys.length == tags2Keys.length &&
      tagsKeys.every((String key) => tags2[key] == tags[key]));
}

bool setStrictEqual(Set? source, Set? compare) {
  if (source == null && compare == null) {
    return true;
  }
  if (source == null || compare == null) {
    return false;
  }
  if (source.length != compare.length) {
    return false;
  }

  var sourceList = source.toList();
  var compareList = compare.toList();
  for (int idx = 0; idx < sourceList.length; idx++) {
    if (sourceList[idx] != compareList[idx]) {
      return false;
    }
  }
  return true;
}
