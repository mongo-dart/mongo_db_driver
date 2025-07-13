import 'package:logging/logging.dart';
import 'package:mongo_db_query/mongo_db_query.dart';

import '../command/base/db_admin_command_operation.dart';
import '../command/base/operation_base.dart';
import '../command/mixin/timing_result.dart';
import '../database/database_exp.dart';
import 'mongo_client_debug_options.dart';
import '../session/session_options.dart';
import '../topology/unknown.dart';
import '../command/command_exp.dart';
import '../core/auth/auth.dart';
import '../core/info/client_auth.dart';
import '../server_side/server_session_pool.dart';
import '../session/client_session.dart';
import '../core/error/mongo_dart_error.dart';
import '../topology/abstract/topology.dart';
import '../settings/default_settings.dart';
import 'mongo_client_options.dart';
import '../utils/decode_dns_seed_list.dart';
import '../utils/decode_url_parameters.dart';
import '../utils/split_hosts.dart';

abstract class DriverInfo {
  String? name;
  String? version;
  String? platform;
}

class Auth {
  /// The username for auth
  String? username;

  /// The password for auth
  String? password;
}

// Reserved chars in URL: $ : / ? # [ ] @
const _dollar = '%24';
const _colon = '%3A';
const _slash = '%2F';
const _questionMark = '%3F';
const _hash = '%23';
const _openSquareBracket = '%5B';
const _closeSquareBracket = '%5D';
const _atSign = '%40';

String encodeUserinfo(String url) {
  Uri.encodeFull(uri)
  return url;
}

class MongoClient {
  // This url can be informed both with the Standard
  /// Connection String Format (`mongodb://`) or with the DNS Seedlist
  /// Connection Format (`mongodb+srv://`).
  /// The former has the format:
  /// mongodb://[username:password@]host1[:port1]
  ///      [,...hostN[:portN]][/[defaultauthdb][?options]]
  /// The latter is available from version 3.6. The format is:
  /// mongodb+srv://[username:password@]host1[:port1]
  ///      [/[databaseName][?options]]
  /// More info are available [here](https://docs.mongodb.com/manual/reference/connection-string/)
  MongoClient(this.url,
      {MongoClientOptions? mongoClientOptions,
      MongoClientDebugOptions? debugOptions}) {
    this.mongoClientOptions = mongoClientOptions ?? MongoClientOptions();
    this.debugOptions = debugOptions ?? MongoClientDebugOptions();
    var uri = Uri.parse(url);
    if (uri.scheme != 'mongodb' && uri.scheme != 'mongodb+srv') {
      throw MongoDartError(
          'The only valid schemas for Db are: "mongodb" and "mongodb+srv".');
    }
    serverSessionPool = ServerSessionPool(this);

    hierarchicalLoggingEnabled = true;

    void listener(LogRecord r) {
      var name = r.loggerName;
      print('${r.time}: $name: ${r.message}');
    }

    Logger.root.onRecord.listen(listener);
  }

  final Logger log = Logger('Mongo Client');

  String url;
  late final MongoClientOptions mongoClientOptions;
  late final MongoClientDebugOptions debugOptions;
  ClientAuth? clientAuth;
  final List<Uri> seedServers = <Uri>[];
  Topology? topology;
  String defaultDatabaseName = defMongoDbName;
  String defaultAuthDbName = defMongoAuthDbName;

  late ServerSessionPool serverSessionPool;
  Set<ClientSession> activeSessions = <ClientSession>{};

  $ClusterTime? clientClusterTime;

  WriteConcern? get writeConcern => mongoClientOptions.writeConcern;
  ReadConcern? get readConcern => mongoClientOptions.readConcern;
  ReadPreference? get readPreference => mongoClientOptions.readPreference;

  ServerApi? get serverApi => mongoClientOptions.serverApi;

  Set<MongoDatabase> databases = <MongoDatabase>{};

  AuthenticationScheme? get authenticationScheme =>
      mongoClientOptions.authenticationMechanism;

  /// It is set to true if Authentication is required and at list one
  /// coonection has been authenticated (that proves that it is possible).
  bool get isAuthenticated => clientAuth?.isAuthenticated ?? false;

  /// return true if hte authetication is required (defined username or X509)
  bool get isAuthenticationRequired {
    bool isX509 = authenticationScheme == AuthenticationScheme.X509;
    String? user = clientAuth?.userName;
    return isX509 || user != null;
  }

  /// Connects to the required server / cluster.
  ///
  /// Steps:
  /// 1) Decode mongodb+srv url if it is the case
  /// 2) Decode the mongodb url
  /// 3) try a connection with the seed list servers
  /// 4) run hello command and determine the topology.
  /// 5) creates the topology.
  Future connect() async {
    var tempSeedList = <Uri>[];
    var connectionUri = Uri.parse(url);

    var hostsSeedList = <String>[];
    if (connectionUri.scheme == 'mongodb+srv') {
      if (mongoClientOptions.directConnection) {
        throw MongoDartError('SRV URI does not support directConnection');
      }
      hostsSeedList.addAll(await decodeDnsSeedlist(connectionUri));
    } else {
      hostsSeedList.addAll(splitHosts(url));
    }
    tempSeedList
        .addAll([for (var element in hostsSeedList) Uri.parse(element)]);
    // The host part of the server names must be normalized in lowercases
    // https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.md#initial-servers
    for (Uri serverUri in tempSeedList) {
      seedServers.add(Uri(
          scheme: serverUri.scheme,
          userInfo: serverUri.userInfo,
          host: serverUri.host.toLowerCase(),
          port: serverUri.port,
          pathSegments: serverUri.pathSegments,
          queryParameters: serverUri.queryParameters,
          fragment: serverUri.fragment));
    }

    if (seedServers.isEmpty) {
      throw MongoDartError('Incorrect connection string');
    }
    // TODO, this test shoul be done on the effective server,
    // and not on the server seeds.
    // https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.md#handling-of-srv-uris-resolving-to-single-host
    if (mongoClientOptions.directConnection && seedServers.length > 1) {
      throw MongoDartError('DirectConnection option requires exactly one host');
    }

    // https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.md#allowed-configuration-combinations
    if (mongoClientOptions.loadBalanced &&
        (mongoClientOptions.directConnection ||
            mongoClientOptions.replicaSet != null)) {
      throw MongoDartError('Load Balanced option cannot '
          'be used with direct Connection or Replica Set');
    }

    clientAuth =
        await decodeUrlParameters(seedServers.first, mongoClientOptions);
    defaultDatabaseName = mongoClientOptions.defaultDbName ?? defMongoDbName;

    TopologyType? type;

    // Initial TopologyType

    // If the directConnection URI option is specified when a MongoClient is
    // constructed, the TopologyType must be initialized based on the value of
    // the directConnection option and the presence of the replicaSet option
    // according to the following table:
    // | directConnection |	replicaSet present |	Initial TopologyType |
    // |       true 	    |         no  	     |        Single         |
    // |       true 	    |        yes 	       |        Single         |
    // |       false 	    |         no 	       |        Unknown        |
    // |       false 	    |        yes 	       |   ReplicaSetNoPrimary |
    //
    // https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.md#initial-topologytype
    if (mongoClientOptions.directConnection) {
      type = TopologyType.single;
    } else {
      if (mongoClientOptions.replicaSet == null) {
        type = TopologyType.unknown;
      } else {
        type = TopologyType.replicaSetNoPrimary;
      }
    }

    var discoverTopology = Unknown(this, seedServers, topologyType: type);

    await discoverTopology.connect();

    topology = await discoverTopology.getEffectiveTopology();

    await _authenticateUser();
  }

  Future<void> _authenticateUser() async {
    if (!isAuthenticationRequired) {
      log.fine(() => 'No Authentication needed for client');
    } else {
      if (mongoClientOptions.authenticationMechanism ==
          AuthenticationScheme.X509) {
        await authenticateX509();
      } else {
        await authenticate(clientAuth?.userName, clientAuth?.password);
      }
      log.fine(() => 'client authenticated');
    }
  }

  // TODO clean the serverSessionPool
  /// Client closing
  ///
  /// When a client is closing, before it emits the TopologyClosedEvent as per
  /// the Events API, it SHOULD remove all servers from its
  /// TopologyDescription and set its TopologyType to Unknown, emitting the
  /// corresponding TopologyDescriptionChangedEvent.
  // https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.md#client-closing
  Future close() async {
    await topology?.close();
    topology = null;
  }

  /// If no name passed, the url specified db is used
  MongoDatabase db({String? dbName}) {
    dbName ??= defaultDatabaseName;
    try {
      return databases.firstWhere((element) => element.databaseName == dbName);
    } catch (_) {}
    var db = MongoDatabase(this, dbName);
    databases.add(db);
    return db;
  }

  // TODO
  ClientSession startSession({SessionOptions? clientSessionOptions}) =>
      ClientSession(this, sessionOptions: clientSessionOptions);

  /// Method for authentication with X509 certificate.
  /// In the conection parameters you have not to set
  /// X509 if you want to use this delayed auth function.

  Future<bool> authenticateX509({ClientSession? session}) async =>
      authenticate(null, null,
          authScheme: AuthenticationScheme.X509,
          authDb: r'$external',
          session: session);

  Future<bool> authenticate(String? userName, String? password,
      {ClientSession? session,
      AuthenticationScheme? authScheme,
      String? authDb}) async {
    if (topology == null) {
      return false;
    }
    var server = topology!.getServer();
    var retValue = false;
    try {
      retValue = await server.authenticate(userName, password,
          session: session, authScheme: authScheme, authDb: authDb);
    } catch (e) {
      /// Atlas does not currently support SHA_256
      if (e is MongoDartError &&
          e.mongoCode == 8000 &&
          e.errorCodeName == 'AtlasError' &&
          e.message.contains('SCRAM-SHA-256') &&
          mongoClientOptions.authenticationMechanism ==
              AuthenticationScheme.SCRAM_SHA_256) {
        log.warning(() => 'Atlas connection: SCRAM_SHA_256 not available, '
            'downgrading to SCRAM_SHA_1');
        mongoClientOptions.authenticationMechanism =
            AuthenticationScheme.SCRAM_SHA_1;
        try {
          await authenticate(clientAuth?.userName!, clientAuth?.password ?? '');
          log.fine(
              () => 'mongoClient.mongoClientOptions.authenticationMechanism: '
                  'client authenticated');
          return false;
        } catch (e) {
          rethrow;
        }
      }

      rethrow;
    }
    return retValue;
  }

  /// Runs a database command
  Future<MongoDocument> adminCommand(Command command,
          {ClientSession? session}) =>
      DbAdminCommandOperation(this, command,
          session: session, options: <String, dynamic>{}).process();

/* 
  void selectAuthenticationMechanism(String authenticationSchemeName) {
    if (authenticationSchemeName == ScramSha1Authenticator.name) {
      authenticationScheme = AuthenticationScheme.SCRAM_SHA_1;
    } else if (authenticationSchemeName == ScramSha256Authenticator.name) {
      authenticationScheme = AuthenticationScheme.SCRAM_SHA_256;
    } else if (authenticationSchemeName == X509Authenticator.name) {
      authenticationScheme = AuthenticationScheme.X509;
    } else {
      throw MongoDartError('Provided authentication scheme is '
          'not supported : $authenticationSchemeName');
    }
  } */
  Future<Map<String, dynamic>> listDatabases(
      {ClientSession? session,
      ListDatabasesOptions? listDatabasesOptions,
      Map<String, Object>? rawOptions}) async {
    var command = ListDatabasesCommand(this,
        session: session,
        listDatabasesOptions: listDatabasesOptions,
        rawOptions: rawOptions);
    return command.process();
  }
}
