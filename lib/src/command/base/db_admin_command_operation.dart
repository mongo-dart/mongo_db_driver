import 'package:logging/logging.dart';
import 'package:meta/meta.dart';
import 'package:mongo_db_query/mongo_db_query.dart';

import '../../core/error/mongo_dart_error.dart';
import '../../mongo_client.dart';
import '../../session/client_session.dart';
import '../../topology/server.dart';
import '../../utils/map_keys.dart';
import 'operation_base.dart';

Logger _log = Logger('Admin Command');

base class DbAdminCommandOperation extends OperationBase {
  DbAdminCommandOperation(this.client, this.command,
      {super.session, super.options})
      : super(client) {
    _debugInfo = client.debugOptions.commandExecutionLogLevel != Level.OFF;
    if (_debugInfo) {
      _log.level = client.debugOptions.commandExecutionLogLevel;
    }
  }

  MongoClient client;
  Command command;
  bool _debugInfo = false;

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

    if (_debugInfo) {
      _log.fine('Command: $command');
    }

    return server.executeCommand(command, this);
  }
}
