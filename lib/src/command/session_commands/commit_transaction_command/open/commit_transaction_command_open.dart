import 'package:mongo_db_driver/src/command/session_commands/commit_transaction_command/base/commit_transaction_command.dart';

import 'commit_transaction_options_open.dart';

base class CommitTransactionCommandOpen extends CommitTransactionCommand {
  CommitTransactionCommandOpen(super.client, super.transactionInfo,
      {super.session,
      CommitTransactionOptionsOpen? commitTransactionOptions,
      super.rawOptions})
      : super.protected(commitTransactionOptions: commitTransactionOptions);
}
