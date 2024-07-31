import 'dart:async';

import 'package:logging/logging.dart';
import 'package:meta/meta.dart';

import '../../command/command_exp.dart';
import '../../core/error/mongo_dart_error.dart';
import '../../core/info/server_config.dart';
import '../../client/mongo_client.dart';
import '../../settings/default_settings.dart';
import '../../client/mongo_client_options.dart';
import '../server.dart';

enum TopologyType {
  single,
  replicaSetNoPrimary,
  replicaSetWithPrimary,
  sharded,
  loadBalanced,
  unknown
}

abstract class Topology {
  @protected
  Topology.protected(this.mongoClient, this.hostsSeedList,
      {List<Server>? detectedServers, required this.type}) {
    if (detectedServers != null) {
      servers.addAll(detectedServers);
    }
  }

  final log = Logger('Topology');
  TopologyType type;
  final List<Uri> hostsSeedList;
  final MongoClient mongoClient;
  MongoClientOptions get mongoClientOptions => mongoClient.mongoClientOptions;
  bool endMonitoring = false;

  /// Returns the primary writable server
  Server? primary;

  List<Server> servers = <Server>[];

  /// This value is updated from the servers when receiving the hello
  /// response. The smallest reported timeout is stored.
  Duration? logicalSessionTimeoutMinutes = defLogicalSessionTimeoutMinutes;

  List<Uri> get seedList => hostsSeedList.toList();
  bool get isConnected => servers.any((element) => element.isConnected);

  // *** To be overridden. This behavior works just for standalone topology
  /// The return value depends on the topology
  /// In case the topology is not connected, the return values is meaningless.
  /// - standalone -> returns readOnly state of the server
  /// - replicaSet -> true if the topology is connected but no primary it is, otherwise false
  /// - sharderCluster -> true if all the mongos are in readOnlyMode.
  bool get isReadOnly => isConnected ? servers.first.isReadOnlyMode : true;

  // *** To be overridden. This behavior works just for standalone typology
  /// Retruns the server based on the readPreference
  Server getServer(
          {ReadPreferenceMode? readPreferenceMode =
              ReadPreferenceMode.primary}) =>
      isConnected ? servers.first : throw MongoDartError('No primary detected');

  Future connect() async {
    if (servers.isEmpty) {
      await addServersFromSeedList();
      await updateServersStatus();
    }
    unawaited(monitorServers());
  }

  Future<void> addServersFromSeedList() async {
    for (var element in hostsSeedList) {
      // TODO Check serverConfig
      var serverConfig = await parseUri(element, mongoClientOptions);
      var server = Server(
          mongoClient, serverConfig, mongoClientOptions.connectionPoolSettings);
      servers.add(server);
      await server.connect();
    }
  }

  @protected
  Future<void> updateServersStatus() async {
    var additionalServers = <Server>{};
    for (var server in servers) {
      if (!server.isConnected) {
        await server.connect();
      } else {
        await server.refreshStatus();
      }
      if (!mongoClientOptions.directConnection) {
        additionalServers
            .addAll(await addOtherServers(server, additionalServers));
      }
    }

    for (var server in additionalServers) {
      if (!server.isConnected) {
        await server.connect();
      } else {
        await server.refreshStatus();
      }
      servers.add(server);
    }
    var detectedLogicalSessionTimeoutMinutes = Duration(days: 1);
    for (var server in servers) {
      if (!server.isConnected) {
        continue;
      }
      int calculatedMinutes = (server.hello?.logicalSessionTimeoutMinutes ??
          defLogicalSessionTimeoutMinutes.inMinutes);
      if (calculatedMinutes < detectedLogicalSessionTimeoutMinutes.inMinutes) {
        detectedLogicalSessionTimeoutMinutes =
            Duration(minutes: calculatedMinutes);
      }
    }
    logicalSessionTimeoutMinutes = detectedLogicalSessionTimeoutMinutes;
  }

  // *** To be overridden. This behavior works just for standalone typology
  @protected
  Future<Set<Server>> addOtherServers(
          Server server, Set<Server> additionalServers) async =>
      <Server>{};

  Future<ServerConfig> parseUri(Uri uri, MongoClientOptions options) async {
    if (uri.scheme != 'mongodb') {
      throw MongoDartError('Invalid scheme in uri: ${uri.scheme}');
    }
/* 
    Uint8List? tlsCAFileContent;
    if (options.tlsCAFile != null) {
      tlsCAFileContent = await File(options.tlsCAFile!).readAsBytes();
    }
    Uint8List? tlsCertificateKeyFileContent;
    if (options.tlsCertificateKeyFile != null) {
      tlsCertificateKeyFileContent =
          await File(options.tlsCertificateKeyFile!).readAsBytes();
    }
    if (options.tlsCertificateKeyFilePassword != null &&
        options.tlsCertificateKeyFile == null) {
      throw MongoDartError('Missing tlsCertificateKeyFile parameter');
    } */

    var serverConfig = ServerConfig(
      host: uri.host,
      port: uri.port,
      /*  isSecure: options.tls,
        tlsAllowInvalidCertificates: options.tlsAllowInvalidCertificates,
        tlsCAFileContent: tlsCAFileContent,
        tlsCertificateKeyFileContent: tlsCertificateKeyFileContent,
        tlsCertificateKeyFilePassword: options.tlsCertificateKeyFilePassword */
    );

    if (serverConfig.port == 0) {
      serverConfig.port = defMongoPort;
    }
/* 
    mongoClient.clientAuth?.userName = options.auth?.username;
    mongoClient.clientAuth?.password = options.auth?.password;
 */
    return serverConfig;
  }

  Server nearest() {
    int? lowestMS;
    Server? selectedServer;
    for (Server server in servers) {
      if (server.isConnected) {
        if (lowestMS == null || server.lastHelloExecutionMS < lowestMS) {
          lowestMS = server.hello!.localTime.millisecondsSinceEpoch;
          selectedServer = server;
        }
      }
    }
    return selectedServer ?? (throw MongoDartError('No server detected'));
  }

  Future<void> monitorServers() async {
    if (endMonitoring) {
      return;
    }
    // TODO parametrize the time.
    await Future.delayed(Duration(seconds: 10), updateServersStatus);
    unawaited(monitorServers());
  }

  Future<void> close() async {
    endMonitoring = true;
  }
}
