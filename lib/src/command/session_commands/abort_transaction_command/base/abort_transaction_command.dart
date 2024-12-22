import 'package:meta/meta.dart';
import 'package:mongo_db_driver/src/command/base/db_admin_command_operation.dart';
import '../../../../core/error/mongo_dart_error.dart';
import '../../../../client/mongo_client.dart';
import '../../../../database/server_api_version.dart';
import '../../../../session/session_exp.dart';
import '../../../../utils/map_keys.dart';
import '../../../base/operation_base.dart';
import '../open/abort_transaction_command_open.dart';
import '../v1/abort_transaction_command_v1.dart';
import 'abort_transaction_options.dart';

base class AbortTransactionCommand extends DbAdminCommandOperation {
  @protected
  AbortTransactionCommand.protected(
      MongoClient client, TransactionInfo transactionInfo,
      {super.session,
      AbortTransactionOptions? abortTransactionOptions,
      Options? rawOptions})
      : super(client, <String, dynamic>{
          keyAbortTransaction: 1,
          //keyTxnNumber: transactionInfo.transactionNumber,
          //keyAutoabort: false
        }, options: <String, dynamic>{
          ...transactionInfo.options.getOptions(client),
          ...?abortTransactionOptions?.getOptions(client),
          ...?rawOptions
        });

  factory AbortTransactionCommand(
      MongoClient client, TransactionInfo transactionInfo,
      {ClientSession? session,
      AbortTransactionOptions? abortTransactionOptions,
      Options? rawOptions}) {
    if (client.serverApi != null) {
      switch (client.serverApi!.version) {
        case ServerApiVersion.v1:
          return AbortTransactionCommandV1(client, transactionInfo,
              session: session,
              abortTransactionOptions: abortTransactionOptions?.toV1,
              rawOptions: rawOptions);
        // ignore: unreachable_switch_default
        default:
          throw MongoDartError(
              'Stable Api ${client.serverApi!.version} not managed');
      }
    }
    return AbortTransactionCommandOpen(client, transactionInfo,
        session: session,
        abortTransactionOptions: abortTransactionOptions?.toOpen,
        rawOptions: rawOptions);
  }
}
