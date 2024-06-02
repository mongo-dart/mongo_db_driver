//part of mongo_dart;
import 'package:crypto/crypto.dart' as crypto;
import 'package:sasl_scram/sasl_scram.dart'
    show
        ScramMechanism,
        UsernamePasswordCredential,
        CryptoStrengthStringGenerator;

import '../network/abstract/connection_base.dart';
import 'sasl_authenticator.dart';

class ScramSha256Authenticator extends SaslAuthenticator {
  static String name = 'SCRAM-SHA-256';

  ScramSha256Authenticator(
      UsernamePasswordCredential credential, ConnectionBase connection)
      : super(
            ScramMechanism(
                'SCRAM-SHA-256', // Optionally choose hash method from a list provided by the server
                crypto.sha256,
                credential,
                CryptoStrengthStringGenerator()),
            connection);
}
