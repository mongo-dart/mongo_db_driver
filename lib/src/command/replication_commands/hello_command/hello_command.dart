import 'package:vy_string_utils/vy_string_utils.dart';

import '../../../database/base/mongo_database.dart';
import '../../../topology/server.dart';
import '../../../utils/map_keys.dart';
import '../../base/server_command.dart';
import 'hello_options.dart';
import 'hello_result.dart';

var _command = <String, dynamic>{keyHello: 1};

/// The Hello command takes the following form:
///
/// `db.runCommand( { hello: 1 } )`
///
/// The hello command accepts optional fields saslSupportedMechs:
/// <db.user> to return an additional field hello.saslSupportedMechs
/// in its result and comment <String> to add a log comment associated
/// with the command.
///
/// `db.runCommand( { hello: 1, saslSupportedMechs: "<db.username>",
/// comment: <String> } )`
base class HelloCommand extends ServerCommand {
  HelloCommand(this.server,
      {MongoDatabase? db,
      String? username,
      super.session,
      HelloOptions? helloOptions,
      Map<String, Object>? rawOptions})
      : super(
          server.mongoClient,
          {
            ..._command,
            key$Db: db?.databaseName ?? 'admin',
            if (filled(username))
              keySaslSupportedMechs: '${db?.databaseName ?? 'admin'}.$username'
          },
          options: <String, dynamic>{...?helloOptions?.options, ...?rawOptions},
        ) {
    requiresAuthentication = false;
  }

  Server server;

  Future<HelloResult> executeDocument() async {
    var result = await process();
    return HelloResult(result);
  }

  @override
  Future<Map<String, dynamic>> process() async => super.executeOnServer(server);

  /*  @override
  @Deprecated('Use execute instead')
  Future<Map<String, dynamic>> executeOnServer(Server server,
          {ClientSession? session}) async =>
      throw MongoDartError('Do not use this method, use execute instead'); */
}
