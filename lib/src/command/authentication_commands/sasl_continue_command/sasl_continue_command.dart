import 'dart:convert';
import 'dart:typed_data';

import '../../../core/network/abstract/connection_base.dart';
import '../../../mongo_client.dart';
import '../../../utils/map_keys.dart';
import '../../base/auth_command.dart';
import 'sasl_continue_options.dart';

base class SaslContinueCommand extends AuthCommand {
  SaslContinueCommand(MongoClient client, int conversationId, Uint8List payload,
      {super.session,
      SaslContinueOptions? saslContinueOptions,
      Map<String, Object>? rawOptions,
      ConnectionBase? connection})
      : super(client, <String, dynamic>{
          keySaslContinue: 1,
          keyConversationId: conversationId,
          keyPayload: base64.encode(payload)
        }, options: <String, dynamic>{
          ...?saslContinueOptions?.options,
          ...?rawOptions
        }) {
    requiresAuthentication = false;
  }
}
