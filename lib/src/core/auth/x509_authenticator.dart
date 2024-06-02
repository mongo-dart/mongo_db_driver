import '../../command/authentication_commands/x509/x509_command.dart';
import '../../session/client_session.dart';
import '../../topology/server.dart';
import '../../utils/map_keys.dart';
import '../error/mongo_dart_error.dart';
import 'auth.dart';

class X509Authenticator extends Authenticator {
  X509Authenticator(this.username, super.connection) : super();

  final String? username;
  //MongoDatabase db;
  static final String name = 'MONGODB-X509';

  @override
  Future authenticate(Server server, {ClientSession? session}) async {
    var client = server.mongoClient;
    var command = X509Command(client, name, username);
    var result = await command.execute(server: server, connection: connection);

    if (result[keyOk] == 0.0) {
      throw MongoDartError(result[keyErrmsg],
          mongoCode: result[keyCode],
          errorCode: result[keyCode] == null ? null : '${result[keyCode]}',
          errorCodeName: result[keyCodeName]);
    }
  }
}
