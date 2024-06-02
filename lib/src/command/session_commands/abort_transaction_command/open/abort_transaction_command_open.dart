import 'package:mongo_db_driver/src/command/session_commands/abort_transaction_command/base/abort_transaction_command.dart';

import 'abort_transaction_options_open.dart';

base class AbortTransactionCommandOpen extends AbortTransactionCommand {
  AbortTransactionCommandOpen(super.client, super.transactionInfo,
      {super.session,
      AbortTransactionOptionsOpen? abortTransactionOptions,
      super.rawOptions})
      : super.protected(abortTransactionOptions: abortTransactionOptions);
}
