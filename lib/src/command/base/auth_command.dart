import 'package:mongo_db_driver/mongo_db_driver.dart';
import 'package:mongo_db_driver/src/command/base/server_command.dart';

import '../../core/network/abstract/connection_base.dart';
import '../../topology/server.dart';
import 'operation_base.dart';

/// Run a simple command
///
/// Designed for system commands where Db/Collection are not needed
base class AuthCommand extends ServerCommand {
  AuthCommand(super.mongoClient, super.command,
      {super.options, super.session, super.aspect})
      : super();

  Future<MongoDocument> execute(
      {required Server server, ConnectionBase? connection}) {
    return executeOnServer(server, connection: connection);
  }

  @override
  void processOptions(Command command) {
    // Get the authentication db name we are executing against
    final dbName =
        ((options[keyAuthdb] as String?) ?? mongoClient.defaultAuthDbName);
    options.removeWhere((key, value) => key == keyDbName || key == keyAuthdb);
    command[key$Db] = dbName;
  }
}
