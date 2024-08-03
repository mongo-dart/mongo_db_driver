import 'dart:async';
import 'dart:math';

import 'package:logging/logging.dart';
import 'package:mongo_db_driver/src/core/info/server_description.dart';
import 'package:mongo_db_query/mongo_db_query.dart';
import 'package:sasl_scram/sasl_scram.dart' hide Authenticator;

import '../client/client_exp.dart';
import '../command/command_exp.dart';
import '../session/session_exp.dart';

import '../command/base/operation_base.dart';
import '../core/auth/auth.dart';
import '../core/info/server_capabilities.dart';
import '../core/info/server_config.dart';
import '../core/info/server_status.dart';
import '../core/message/abstract/section.dart';
import '../core/message/mongo_message.dart';
import '../core/network/abstract/connection_base.dart';
import '../core/network/connection_pool.dart';

enum ServerState { closed, connected }

// TODO check how to use this structure
enum ServerType {
  standalone,
  mongos,
  possiblePrimary,
  rsPrimary,
  rsSecondary,
  rsArbiter,
  rsOther,
  rsGhost,
  loadBalancer,
  unknown
}

class Server {
  Server(this.mongoClient, this.serverConfig,
      ConnectionPoolSettings poolSettings) {
    connectionPool = ConnectionPool(this, poolSettings);
  }

  final Logger log = Logger('Server');
  final MongoClient mongoClient;
  final ServerConfig serverConfig;
  final ServerCapabilities serverCapabilities = ServerCapabilities();
  final ServerStatus serverStatus = ServerStatus();
  late ConnectionPool connectionPool;

  ServerState state = ServerState.closed;
  HelloResult? hello;

  int lastHelloExecutionMS = 0;
  List<int> last10HelloExecutionsMS = List<int>.filled(10, 9999999);
  int lastExecutionIdx = 0;
  MongoDartError? lastExecutionError;

  bool get isAuthenticated => mongoClient.isAuthenticated;
  bool get isConnected => state == ServerState.connected;

  bool get isStandalone => serverCapabilities.isStandalone;
  bool get isReplicaSet => serverCapabilities.isReplicaSet;
  bool get isShardedCluster => serverCapabilities.isShardedCluster;

  bool get isWritablePrimary => hello?.isWritablePrimary ?? false;
  bool get isReadOnlyMode => hello?.readOnly ?? true;

  /// Return the server url (no scheme)
  /// Url can be considered correct only after receiving the first hello message
  String get url => hello?.me == null ? serverConfig.hostUrl : hello!.me!;

  /// Comparison operator.
  /// Note, it is correct only after the first hello message (connection)
  /// Do not add to containers before that.
  @override
  bool operator ==(other) => other is Server && url == other.url;

  /// Hash Code.
  /// Note, it is correct only after the first hello message (connection)
  /// Do not add to containers before that.
  @override
  int get hashCode => url.hashCode;

  @override
  String toString() => 'Server -> $url';

  Future<void> connect() async {
    if (state == ServerState.connected) {
      return;
    }
    await connectionPool.connectPool();
    if (!connectionPool.isConnected) {
      throw ConnectionException('No Connection Available');
    }
    state = ServerState.connected;
    await _runHello();
    await ServerStatusCommand(mongoClient,
            serverStatusOptions: ServerStatusOptions.instance)
        .updateServerStatus(this);
  }

  Future<void> close() async {
    await connectionPool.closePool();
    return;
  }

  Future<MongoDocument> executeCommand(Command command, OperationBase operation,
      {ConnectionBase? connection}) async {
    if (state != ServerState.connected) {
      throw MongoDartError('Server is not connected. $state');
    } //var isImplicitSession = session == null;
    if (connection == null) {
      connection = await connectionPool.getAvailableConnection();
    } else {
      if (connection.server != this) {
        throw MongoDartError(
            'Connection received is not for the required server');
      }
    }

    if (mongoClient.isAuthenticationRequired &&
        operation.requiresAuthentication &&
        !connection.isAuthenticated) {
      await _authenticate(mongoClient.clientAuth?.userName,
          mongoClient.clientAuth?.password, connection,
          session: operation.session,
          authScheme: mongoClient.authenticationScheme,
          authDb: mongoClient.defaultAuthDbName);
    }

    operation.session.prepareCommand(command);

    var response = await connection.execute(MongoMessage(command));
    if (operation.isImplicitSession) {
      await operation.session.endSession();
    }

    var section = response.sections.firstWhere((Section section) =>
        section.payloadType == MongoMessage.basePayloadType);
    return section.payload.content;
  }

  Future<void> refreshStatus() => _runHello();

  Future<void> _runHello() async {
    Map<String, dynamic> result = {keyOk: 0.0};
    try {
      var helloCommand =
          HelloCommand(this, username: mongoClient.clientAuth?.userName);
      var actualTimeMS = DateTime.now().millisecondsSinceEpoch;
      try {
        result = await helloCommand.process();
      } catch (error, stackTrace) {
        print(error);
        print(stackTrace);
        rethrow;
      }
      lastHelloExecutionMS =
          DateTime.now().millisecondsSinceEpoch - actualTimeMS;
      if (lastExecutionIdx > 9) {
        lastExecutionIdx = 0;
      }
      last10HelloExecutionsMS[lastExecutionIdx++] = lastHelloExecutionMS;
      lastExecutionError = null;
    } on MongoDartError catch (err) {
      //Do nothing
      print('Passed by _runHello() - Error ${err.message}');
      lastExecutionError = err;
    }
    if (result[keyOk] == 1.0) {
      hello = HelloResult(result);
      if (isWritablePrimary) {
        MongoMessage.maxBsonObjectSize = hello!.maxBsonObjectSize;
        MongoMessage.maxMessageSizeBytes = hello!.maxMessageSizeBytes;
        MongoMessage.maxWriteBatchSize = hello!.maxWriteBatchSize;
      }
      serverCapabilities.getParamsFromHello(hello!);

      if (mongoClient.mongoClientOptions.authenticationMechanism == null &&
          hello?.saslSupportedMechs != null) {
        if (hello!.saslSupportedMechs!.contains('SCRAM-SHA-256')) {
          mongoClient.mongoClientOptions.authenticationMechanism =
              AuthenticationScheme.SCRAM_SHA_256;
        } else if (hello!.saslSupportedMechs!.contains('SCRAM-SHA-1')) {
          mongoClient.mongoClientOptions.authenticationMechanism =
              AuthenticationScheme.SCRAM_SHA_1;
        } else {
          AuthenticationScheme.SCRAM_SHA_1;
        }
      }
    }
  }

  Future<bool> authenticate(
    String? userName,
    String? password, {
    ClientSession? session,
    AuthenticationScheme? authScheme,
    String? authDb,
  }) async {
    var connection = await connectionPool.getAvailableConnection();
    return _authenticate(userName, password, connection,
        session: session, authScheme: authScheme, authDb: authDb);
  }

  /// For internal use, allows to specify a connection
  Future<bool> _authenticate(
    String? userName,
    String? password,
    ConnectionBase connection, {
    ClientSession? session,
    AuthenticationScheme? authScheme,
    String? authDb,
  }) async {
    var credential = UsernamePasswordCredential()
      ..username = userName
      ..password = password;

    mongoClient.clientAuth?.userName ??= userName;
    mongoClient.clientAuth?.password ??= password;

    if (authScheme != null) {
      mongoClient.mongoClientOptions.authenticationMechanism = authScheme;
    }
    if (authDb != null) {
      mongoClient.defaultAuthDbName = authDb;
    }
    if (mongoClient.authenticationScheme == null) {
      throw MongoDartError('Authentication scheme not specified');
    }

    var authenticator = Authenticator.create(
        mongoClient.authenticationScheme!, connection, credential);

    await authenticator.authenticate(this, session: session);

    mongoClient.clientAuth?.isAuthenticated = true;
    connection.isAuthenticated = true;

    return true;
  }

  ServerDescription get serverDescription {
    return ServerDescription(serverConfig.hostUrl,
        options: ServerDescriptionOptions()
          ..error = lastExecutionError
          ..roundTripTime = lastHelloExecutionMS
          ..minRoundTripTime = last10HelloExecutionsMS
              .reduce((value, element) => min(value, element))
          ..loadBalanced = MongoClientOptions().loadBalanced,
        type: serverType,
        hosts: hello?.hosts,
        arbiters: hello?.arbiters,
        passives: hello?.passives,
        tags: hello?.tags,
        topologyVersion: hello?.topologyVersion,
        minWireVersion: hello?.minWireVersion,
        maxWireVersion: hello?.maxWireVersion,
        lastWrite: hello?.lastWrite,
        me: hello?.me,
        primary: hello?.primary,
        setName: hello?.setName,
        setVersion: hello?.setVersion,
        electionId: hello?.electionId,
        logicalSessionTimeoutMinutes: hello?.logicalSessionTimeoutMinutes,
        operationTime: hello?.operationTime,
        $clusterTime: hello?.$clusterTime);
  }

// Parses a `hello` message and determines the server type
  ServerType get serverType {
    if (mongoClient.mongoClientOptions.loadBalanced) {
      return ServerType.loadBalancer;
    }

    if (hello == null) {
      return ServerType.unknown;
    }
    if (hello!.failure) {
      return ServerType.unknown;
    }

    if (hello!.isreplicaset) {
      return ServerType.rsGhost;
    }

    if (hello!.msg != null && hello!.msg == 'isdbgrid') {
      return ServerType.mongos;
    }

    if (hello!.setName != null) {
      if (hello!.hidden ?? false) {
        return ServerType.rsArbiter;
      } else if (hello!.isWritablePrimary) {
        return ServerType.rsPrimary;
      } else if (hello!.secondary ?? false) {
        return ServerType.rsSecondary;
      } else if (hello!.arbiterOnly ?? false) {
        return ServerType.rsArbiter;
      } else {
        return ServerType.rsOther;
      }
    }

    return ServerType.standalone;
  }
}
