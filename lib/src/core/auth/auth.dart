import 'package:mongo_db_driver/src/session/client_session.dart';
import 'package:mongo_db_driver/src/topology/server.dart';
import 'package:sasl_scram/sasl_scram.dart' show UsernamePasswordCredential;

import '../error/mongo_dart_error.dart';

import '../network/abstract/connection_base.dart';
import 'scram_sha1_authenticator.dart';
import 'scram_sha256_authenticator.dart';
import 'x509_authenticator.dart';

// ignore: constant_identifier_names
enum AuthenticationScheme { MONGODB_CR, SCRAM_SHA_1, SCRAM_SHA_256, X509 }

abstract class Authenticator {
  Authenticator(this.connection);

  factory Authenticator.create(AuthenticationScheme authenticationScheme,
      ConnectionBase connection, UsernamePasswordCredential credentials) {
    switch (authenticationScheme) {
      /*  case AuthenticationScheme.MONGODB_CR:
        return MongoDbCRAuthenticator(credentials, connection); */
      case AuthenticationScheme.SCRAM_SHA_1:
        return ScramSha1Authenticator(credentials, connection);
      case AuthenticationScheme.SCRAM_SHA_256:
        return ScramSha256Authenticator(credentials, connection);
      case AuthenticationScheme.X509:
        return X509Authenticator(credentials.username, connection);
      default:
        throw MongoDartError("Authenticator wasn't specified");
    }
  }

  static String? name;
  ConnectionBase connection;

  Future authenticate(Server server, {ClientSession? session});
}

abstract class RandomStringGenerator {
  static const String allowedCharacters = '!"#\'\$%&()*+-./0123456789:;<=>?@'
      'ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~';

  String generate(int length);
}

Map<String, String> parsePayload(String payload) {
  var dict = <String, String>{};
  var parts = payload.split(',');

  for (var i = 0; i < parts.length; i++) {
    var key = parts[i][0];
    var value = parts[i].substring(2);
    dict[key] = value;
  }

  return dict;
}
