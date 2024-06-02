import 'package:mongo_db_driver/src/command/session_commands/commit_transaction_command/base/commit_transaction_options.dart';

class CommitTransactionOptionsOpen extends CommitTransactionOptions {
  const CommitTransactionOptionsOpen({super.writeConcern, super.comment})
      : super.protected();
}
