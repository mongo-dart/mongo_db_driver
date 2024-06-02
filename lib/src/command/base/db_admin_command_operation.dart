import 'package:meta/meta.dart';
import 'package:mongo_db_driver/mongo_db_driver.dart';

import '../../session/client_session.dart';
import '../../topology/server.dart';
import 'operation_base.dart';

base class DbAdminCommandOperation extends OperationBase {
  DbAdminCommandOperation(this.client, this.command,
      {super.session, super.options})
      : super(client);

  MongoClient client;
  Command command;

  Command $buildCommand() => command;

  @override
  Future<MongoDocument> process() async => executeOnServer(
      client.topology
              ?.getServer(readPreferenceMode: client.readPreference?.mode) ??
          (throw MongoDartError('Server not found')),
      session: session);

  /// This method is for exposing a common interface for the user
  /// Must be overriden from commands
  Future<MongoDocument> execute({Server? server}) {
    if (server == null) {
      return process();
    }
    return executeOnServer(server);
  }

  @override
  @nonVirtual
  @protected
  Future<MongoDocument> executeOnServer(Server server,
      {ClientSession? session}) async {
    var command = <String, dynamic>{...$buildCommand(), key$Db: 'admin'};
    options.removeWhere((key, value) => command.containsKey(key));

    if (client.serverApi != null) {
      command.addAll(client.serverApi!.options);
    }

    command.addAll(options);

    return server.executeCommand(command, this);
  }
}
