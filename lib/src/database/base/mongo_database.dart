//part of mongo_dart;

import 'package:logging/logging.dart';
import 'package:meta/meta.dart';
import 'package:mongo_db_query/mongo_db_query.dart';

import '../../command/command_exp.dart';
import '../../core/error/mongo_dart_error.dart';
import '../../client/mongo_client.dart';
import '../../unions/hint_union.dart';
import '../../command/base/operation_base.dart';
import '../../session/client_session.dart';
import '../../topology/abstract/topology.dart';
import '../../utils/map_keys.dart';
import '../../command/base/command_operation.dart';
import '../../topology/server.dart';
import '../database_exp.dart';

class MongoDatabase {
  @protected
  MongoDatabase.protected(this.mongoClient, this.databaseName);

  factory MongoDatabase(MongoClient mongoClient, String databaseName) {
    // Todo if the serverApi will be available also by Database
    //      receive tha appropriate parameter ad use it instead of
    //      the one from the client class
    if (mongoClient.serverApi != null) {
      switch (mongoClient.serverApi!.version) {
        case ServerApiVersion.v1:
          return MongoDatabaseV1(mongoClient, databaseName);
        default:
          throw MongoDartError(
              'Stable Api ${mongoClient.serverApi!.version} not managed');
      }
    }
    return MongoDatabaseOpen(mongoClient, databaseName);
  }

  final log = Logger('Db');
  final List<String> _uriList = <String>[];
  late MongoClient mongoClient;

  //State state = State.init;
  String? databaseName;
  String? _debugInfo;
  //MongoDatabase? authSourceDb;

  WriteConcern? _writeConcern;
  ReadConcern? _readConcern;
  ReadPreference? readPreference;

  //Todo temp solution
  Server get server => topology.getServer();
  Topology get topology =>
      mongoClient.topology ??
      (throw MongoDartError('Topology not yet assigned'));

  @override
  String toString() => 'Db($databaseName,$_debugInfo)';

  /// Sets the readPreference at Database level
  void setReadPref(ReadPreference? readPreference) =>
      this.readPreference = readPreference;

  /// Runs a database command
  Future<MongoDocument> runCommand(Command command, {ClientSession? session}) =>
      CommandOperation(this, command, <String, dynamic>{}, session: session)
          .process();

  /// Creates a collection object
  MongoCollection collection(String collectionName) =>
      MongoCollection(this, collectionName);

  /// At present it can be defined only at client level
  ServerApi? get serverApi => mongoClient.serverApi;

  WriteConcern? get writeConcern => _writeConcern ?? mongoClient.writeConcern;
  ReadConcern? get readConcern => _readConcern ?? mongoClient.readConcern;

  // ********************************************************************
  // ********************    C O M M A N D S   **************************
  // ********************************************************************

  Future<bool> drop(String collectionName,
      {ClientSession? session,
      DropOptions? dropOptions,
      Map<String, Object>? rawOptions}) async {
    var result = await dropCollection(collectionName,
        session: session, dropOptions: dropOptions, rawOptions: rawOptions);
    return result[keyOk] == 1.0;
  }

  /// This method drops a collection
  Future<Map<String, dynamic>> dropCollection(String collectionName,
      {ClientSession? session,
      DropOptions? dropOptions,
      Map<String, Object>? rawOptions}) async {
    var command = DropCommand(this, collectionName,
        session: session, dropOptions: dropOptions, rawOptions: rawOptions);
    return command.process();
  }

  /// Drop current database
  Future dropDatabase() async {
    var result = await dropDb();
    return result[keyOk] == 1.0;
  }

  /// This method drops the current DB
  /// The difference with the Drop Database command is that this
  /// version returns the server response,
  /// while the other only a boolean.
  Future<Map<String, dynamic>> dropDb(
      {ClientSession? session,
      DropDatabaseOptions? dropOptions,
      Map<String, Object>? rawOptions}) async {
    var command = DropDatabaseCommand(this,
        session: session,
        dropDatabaseOptions: dropOptions,
        rawOptions: rawOptions);
    return command.process();
  }

  Future<List<Map<String, dynamic>>> getCollectionInfos(
          [Map<String, dynamic> filter = const {}]) async =>
      listCollections(filter: filter).toList();

  Future<List<String?>> getCollectionNames(
      [Map<String, dynamic> filter = const {}]) async {
    var ret = await listCollections().toList();

    return [
      for (var element in ret)
        for (var nameKey in element.keys)
          if (nameKey == keyName) element[keyName]
    ];
  }

  MongoDatabase getSibling(String dbName) => mongoClient.db(dbName: dbName);

  List<String> get uriList => _uriList.toList();

  // TODO session needed ?
  /// Ping command
  Future<MongoDocument> pingCommand({ClientSession? session}) =>
      PingCommand(mongoClient).process();

  // ********************************************************************
  // ********************          OLD          *************************
  // ********************************************************************

  /*  @Deprecated('No More Used')
  Future<MongoReplyMessage> queryMessage(MongoMessage queryMessage,
      {ConnectionBase? connection}) {
    throw MongoDartError('No More used');
  } */

  /*  @Deprecated('No More Used')
  void executeMessage(MongoMessage message, WriteConcern? writeConcern,
      {ConnectionBase? connection}) {
    throw MongoDartError('No More used');
  } */

  /*  @Deprecated('Do Not Use')
  Future open(Server server,
      {ConnectionBase? connection,
      WriteConcern writeConcern = WriteConcern.acknowledged,
      bool secure = false,
      bool tlsAllowInvalidCertificates = false,
      String? tlsCAFile,
      String? tlsCertificateKeyFile,
      String? tlsCertificateKeyFilePassword}) async {
    throw MongoDartError('No More Used');
  } */

  /*  @Deprecated('No More Used')
  Future<Map<String, dynamic>> executeDbCommand(MongoMessage message,
      {ConnectionBase? connection}) async {
    throw MongoDartError('No More used');
  } */

  bool documentIsNotAnError(firstRepliedDocument) =>
      firstRepliedDocument['ok'] == 1.0 && firstRepliedDocument['err'] == null;

  /*  @Deprecated('Deprecated since version 4.0.')
  Future<Map<String, dynamic>> getNonce({ConnectionBase? connection}) {
    throw MongoDartError('getnonce command not managed in this version');
  } */

  // TODO new version needed?
  /*  @Deprecated('No More Used')
  Future<Map<String, dynamic>> getBuildInfo({ConnectionBase? connection}) {
    throw MongoDartError('No More used');
  } */

  /*  @Deprecated('No More Used')
  Future<Map<String, dynamic>> wait() => throw MongoDartError('No More used');
 */
  // TODO new version ?
  /// Analogue to shell's `show dbs`. Helper for `listDatabases` mongodb command.
  /* @Deprecated('No More Used')
  Future<List> listDatabases() async {
    throw MongoDartError('No More used');
  } */

  String _createIndexName(Map<String, dynamic> keys) {
    var name = '';

    keys.forEach((key, value) {
      if (name.isEmpty) {
        name = '${key}_$value';
      } else {
        name = '${name}_${key}_$value';
      }
    });

    return name;
  }

  Future<Map<String, dynamic>> createIndex(String collectionName,
      {String? key,
      Map<String, dynamic>? keys,
      bool? unique,
      bool? sparse,
      bool? background,
      bool? dropDups,
      Map<String, dynamic>? partialFilterExpression,
      String? name}) {
    return collection(collectionName).createIndex(
        key: key,
        keys: keys,
        unique: unique,
        sparse: sparse,
        background: background,
        dropDups: dropDups,
        partialFilterExpression: partialFilterExpression,
        name: name);
  }

  Map<String, dynamic> _setKeys(String? key, Map<String, dynamic>? keys) {
    if (key != null && keys != null) {
      throw ArgumentError('Only one parameter must be set: key or keys');
    }

    if (key != null) {
      keys = {};
      keys[key] = 1;
    }

    if (keys == null) {
      throw ArgumentError('key or keys parameter must be set');
    }

    return keys;
  }

  Future ensureIndex(String collectionName,
      {String? key,
      Map<String, dynamic>? keys,
      bool? unique,
      bool? sparse,
      bool? background,
      bool? dropDups,
      Map<String, dynamic>? partialFilterExpression,
      String? name}) async {
    keys = _setKeys(key, keys);
    var indexInfos = await collection(collectionName).getIndexes();

    name ??= _createIndexName(keys);

    if (indexInfos.any((info) => info['name'] == name) ||
        // For compatibility reasons, old indexes where created with
        // a leading underscore
        indexInfos.any((info) => info['name'] == '_$name')) {
      return {'ok': 1.0, 'result': 'index preexists'};
    }

    var createdIndex = await createIndex(collectionName,
        keys: keys,
        unique: unique,
        sparse: sparse,
        background: background,
        dropDups: dropDups,
        partialFilterExpression: partialFilterExpression,
        name: name);

    return createdIndex;
  }

  // **********************************************************+
  // ************** OP_MSG_COMMANDS ****************************
  // ***********************************************************

  /// This method return the status information on the
  /// connection.
  ///
  /// Only works from version 3.6
  Future<Map<String, dynamic>> serverStatus(
      {ClientSession? session, Map<String, Object>? options}) async {
    var operation = ServerStatusCommand(mongoClient,
        session: session, serverStatusOptions: ServerStatusOptions.instance);
    return operation.process();
  }

  /// This method explicitly creates a collection
  Future<Map<String, dynamic>> createCollection(String name,
      {ClientSession? session,
      CreateCollectionOptions? createCollectionOptions,
      Map<String, Object>? rawOptions}) async {
    var command = CreateCollectionCommand(this, name,
        createCollectionOptions: createCollectionOptions,
        rawOptions: rawOptions);
    return command.process();
  }

  /// This method retuns a stream cursor to get a list of the collections
  /// for this DB.
  ///
  Stream<Map<String, dynamic>> listCollections(
      {QueryExpression? selector,
      Map<String, dynamic>? filter,
      ListCollectionsOptions? findOptions,
      Map<String, Object>? rawOptions}) {
    var command = ListCollectionsCommand(this,
        filter: filter ??
            (selector?.rawFilter == null ? null : selector!.rawFilter),
        listCollectionsOptions: findOptions,
        rawOptions: rawOptions);

    return Cursor(command, server).stream;
  }

  /// This method creates a view
  Future<Map<String, dynamic>> createView(
      String view, String source, List pipeline,
      {ClientSession? session,
      CreateViewOptions? createViewOptions,
      Map<String, Object>? rawOptions}) async {
    var command = CreateViewCommand(this, view, source, pipeline,
        createViewOptions: createViewOptions, rawOptions: rawOptions);
    return command.process();
  }

  // ****************************************************
  // ***********     AGGREGATE    ***********************
  // ****************************************************

  /// This method returns a stream that can be read or transformed into
  /// a list with `.toList()`
  ///
  /// The pipeline can be either an `AggregationPipelineBuilder` or a
  /// List of Maps (`List<Map<String, Object>>`)
  Stream<Map<String, dynamic>> aggregateToStream(dynamic pipeline,
          {bool? explain,
          Map<String, Object>? cursor,
          HintUnion? hint,
          ClientSession? session,
          AggregateOptions? aggregateOptions,
          Map<String, Object>? rawOptions}) =>
      aggregate(pipeline,
              explain: explain,
              cursor: cursor,
              hint: hint,
              session: session,
              aggregateOptions: aggregateOptions,
              rawOptions: rawOptions)
          .stream;

  /// This method returns a curosr that can be read or transformed into
  /// a stream with `stream` (for a stream you can directly call
  /// `aggregateToStream`)
  ///
  /// The pipeline can be either an `AggregationPipelineBuilder` or a
  /// List of Maps (`List<Map<String, Object>>`)
  ///
  /// Runs a specified admin/diagnostic pipeline which does not require an
  /// underlying collection. For aggregations on collection data,
  /// see `dbcollection.aggregate()`.
  Cursor aggregate(dynamic pipeline,
      {bool? explain,
      Map<String, Object>? cursor,
      HintUnion? hint,
      ClientSession? session,
      AggregateOptions? aggregateOptions,
      Map<String, Object>? rawOptions}) {
    return Cursor(
        AggregateOperation(pipeline,
            db: this,
            explain: explain,
            cursor: cursor,
            hint: hint,
            session: session,
            aggregateOptions: aggregateOptions,
            rawOptions: rawOptions),
        server);
  }
}
