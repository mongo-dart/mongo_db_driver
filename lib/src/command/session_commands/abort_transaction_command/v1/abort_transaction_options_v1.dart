import 'package:mongo_db_driver/src/command/session_commands/abort_transaction_command/base/abort_transaction_options.dart';

class AbortTransactionOptionsV1 extends AbortTransactionOptions {
  const AbortTransactionOptionsV1({super.writeConcern, super.comment})
      : super.protected();
}
