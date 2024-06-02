import 'dart:async';

import 'package:logging/logging.dart';
import 'package:mongo_db_driver/mongo_db_driver.dart';
import 'package:mongo_db_driver/src/core/error/connection_exception.dart';
import 'package:mongo_db_driver/src/settings/connection_pool_settings.dart';
import 'package:sasl_scram/sasl_scram.dart' hide Authenticator;

import '../command/base/operation_base.dart';
import '../core/auth/auth.dart';
import '../core/info/server_capabilities.dart';
import '../core/info/server_config.dart';
import '../core/info/server_status.dart';
import '../core/message/abstract/section.dart';
import '../core/message/mongo_message.dart';
import '../core/network/abstract/connection_base.dart';
import '../core/network/connection_pool.dart';
import '../session/client_session.dart';

enum ServerState { closed, connected }

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
    } on MongoDartError catch (err) {
      //Do nothing
      print('Passed by _runHello() - Error ${err.message}');
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
}
