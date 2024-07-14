import 'dart:convert';
import 'dart:typed_data';

import 'package:mongo_db_driver/src/command/base/operation_base.dart';
import 'package:mongo_db_driver/src/core/network/abstract/connection_base.dart';

import '../../../client/mongo_client.dart';
import '../../../utils/map_keys.dart';
import '../../base/auth_command.dart';
import 'sasl_start_options.dart';

base class SaslStartCommand extends AuthCommand {
  SaslStartCommand(MongoClient client, String mechanism, Uint8List payload,
      {super.session,
      SaslStartOptions? saslStartOptions,
      Options? rawOptions,
      ConnectionBase? connection})
      : super(client, <String, dynamic>{
          keySaslStart: 1,
          keyMechanism: mechanism,
          keyPayload: base64.encode(payload)
        }, options: <String, dynamic>{
          ...?saslStartOptions?.options,
          ...?rawOptions
        }) {
    requiresAuthentication = false;
  }
}
