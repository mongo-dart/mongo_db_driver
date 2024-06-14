import 'dart:convert';

import 'package:crypto/crypto.dart' show md5;
import 'package:mongo_db_driver/src/core/auth/scram_sha1_authenticator.dart';
import 'package:sasl_scram/sasl_scram.dart'
    show SaslMechanism, UsernamePasswordCredential;

import 'package:mongo_db_driver/src/core/auth/auth.dart';

import '../../command/base/auth_command.dart';
import '../../command/command.dart';
import '../../session/client_session.dart';
import '../../utils/map_keys.dart';
import '../error/mongo_dart_error.dart';
import '../../topology/server.dart';

abstract class SaslAuthenticator extends Authenticator {
  SaslAuthenticator(this.mechanism, super.connection) : super();

  SaslMechanism mechanism;

  @override
  Future authenticate(Server server, {ClientSession? session}) async {
    var currentStep = mechanism.initialize(specifyUsername: true);
    var client = server.mongoClient;

    AuthCommand command = SaslStartCommand(
        client, mechanism.name, currentStep.bytesToSendToServer,
        saslStartOptions: SaslStartOptions(), session: session);

    while (true) {
      Map<String, dynamic> result;

      result = await command.execute(server: server, connection: connection);

      if (result[keyOk] == 0.0) {
        throw MongoDartError(result[keyErrmsg],
            mongoCode: result[keyCode],
            errorCode: result[keyCode] == null ? null : '${result[keyCode]}',
            errorCodeName: result[keyCodeName]);
      }
      if (result['done'] == true) {
        break;
      }

      var payload = result['payload'];

      var payloadAsBytes = base64.decode(payload.toString());

      if (mechanism.name == ScramSha1Authenticator.name) {
        currentStep = currentStep.transition(payloadAsBytes,
            passwordDigestResolver: (UsernamePasswordCredential credential) =>
                md5
                    .convert(utf8.encode(
                        '${credential.username}:mongo:${credential.password}'))
                    .toString());
      } else {
        currentStep = currentStep.transition(payloadAsBytes);
      }

      var conversationId = result['conversationId'] as int;

      command = SaslContinueCommand(
          client, conversationId, currentStep.bytesToSendToServer);
    }
  }
}
