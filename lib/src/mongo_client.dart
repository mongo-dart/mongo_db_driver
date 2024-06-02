import 'package:logging/logging.dart';

import 'session/session_options.dart';
import 'topology/discover.dart';
import 'command/command.dart';
import 'core/auth/auth.dart';
import 'core/info/client_auth.dart';
import 'server_api.dart';
import 'server_side/server_session_pool.dart';
import 'session/client_session.dart';
import 'core/error/mongo_dart_error.dart';
import 'topology/abstract/topology.dart';
import 'settings/default_settings.dart';
import 'mongo_client_options.dart';
import 'utils/decode_dns_seed_list.dart';
import 'utils/decode_url_parameters.dart';
import 'utils/split_hosts.dart';
import 'database/base/mongo_database.dart';

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
  MongoClient(this.url, {MongoClientOptions? mongoClientOptions}) {
    this.mongoClientOptions = mongoClientOptions ?? MongoClientOptions();
    var uri = Uri.parse(url);
    if (uri.scheme != 'mongodb' && uri.scheme != 'mongodb+srv') {
      throw MongoDartError(
          'The only valid schemas for Db are: "mongodb" and "mongodb+srv".');
    }
    serverSessionPool = ServerSessionPool(this);
  }

  final Logger log = Logger('Mongo Client');

  String url;
  late MongoClientOptions mongoClientOptions;
  ClientAuth? clientAuth;
  final List<Uri> seedServers = <Uri>[];
  Topology? topology;
  String defaultDatabaseName = defMongoDbName;
  String defaultAuthDbName = defMongoAuthDbName;

  late ServerSessionPool serverSessionPool;
  Set<ClientSession> activeSessions = <ClientSession>{};

  DateTime? clientClusterTime;

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
    var connectionUri = Uri.parse(url);

    var hostsSeedList = <String>[];
    if (connectionUri.scheme == 'mongodb+srv') {
      hostsSeedList.addAll(await decodeDnsSeedlist(connectionUri));
    } else {
      hostsSeedList.addAll(splitHosts(url));
    }
    seedServers.addAll([for (var element in hostsSeedList) Uri.parse(element)]);

    clientAuth = await decodeUrlParameters(connectionUri, mongoClientOptions);
    defaultDatabaseName = mongoClientOptions.defaultDbName ?? defMongoDbName;

    var discoverTopology = Discover(this, seedServers);

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
  Future close() async {
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
