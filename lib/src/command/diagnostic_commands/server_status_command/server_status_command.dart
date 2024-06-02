import '../../../mongo_client.dart';
import '../../../topology/server.dart';
import '../../../utils/map_keys.dart';
import '../../base/db_admin_command_operation.dart';
import 'server_status_options.dart';
import 'server_status_result.dart';

var _command = <String, dynamic>{keyServerStatus: 1};

base class ServerStatusCommand extends DbAdminCommandOperation {
  ServerStatusCommand(MongoClient client,
      {super.session,
      ServerStatusOptions? serverStatusOptions,
      Map<String, Object>? rawOptions})
      : super(
          client,
          _command,
          options: <String, dynamic>{
            ...?serverStatusOptions?.options,
            ...?rawOptions
          },
        );

  Future<ServerStatusResult> executeDocument(Server server) async {
    var result = await super.process();
    return ServerStatusResult(result);
  }

  /// Update basic server info + FeatureCompatibilityVersion
  Future<void> updateServerStatus(Server server) async {
    var result = await super.executeOnServer(server);
    // On error the ServerStatus class is not initialized
    // check the `isInitialized` flag.
    //
    // Possible errors are: older version or authorization (requires
    // `clusterMonitor` role if authorization is active)
    server.serverStatus.processServerStatus(result);
  }
}
