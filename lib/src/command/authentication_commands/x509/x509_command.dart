import '../../../core/network/abstract/connection_base.dart';
import '../../../mongo_client.dart';
import '../../../utils/map_keys.dart';
import '../../base/auth_command.dart';
import '../../base/operation_base.dart';
import '../../command.dart';

base class X509Command extends AuthCommand {
  X509Command(MongoClient client, String mechanism, String? username,
      {super.session,
      SaslStartOptions? saslStartOptions,
      Options? rawOptions,
      ConnectionBase? connection})
      : super(client, <String, dynamic>{
          keyAuthenticate: 1,
          keyMechanism: mechanism,
          if (username != null && username.isNotEmpty) keyUser: username
        }, options: <String, dynamic>{
          ...?saslStartOptions?.options,
          ...?rawOptions
        }) {
    requiresAuthentication = false;
  }
}
