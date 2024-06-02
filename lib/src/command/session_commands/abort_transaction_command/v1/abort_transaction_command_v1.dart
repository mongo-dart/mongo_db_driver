import 'package:mongo_db_driver/src/command/session_commands/abort_transaction_command/base/abort_transaction_command.dart';

import 'abort_transaction_options_v1.dart';

base class AbortTransactionCommandV1 extends AbortTransactionCommand {
  AbortTransactionCommandV1(super.client, super.transactionInfo,
      {super.session,
      AbortTransactionOptionsV1? abortTransactionOptions,
      super.rawOptions})
      : super.protected(abortTransactionOptions: abortTransactionOptions);
}
