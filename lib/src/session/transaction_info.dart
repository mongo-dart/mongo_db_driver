import 'package:fixnum/fixnum.dart';
import 'package:mongo_db_driver/src/topology/server.dart';

import 'transaction_options.dart';

enum TransactionState {
  none,
  starting,
  inProgress,
  committed,
  committedEmpty,
  aborted
}

/// This is an internal object, used to collect info and state related to
/// the transaction.
/// [Specifications](https://github.com/mongodb/specifications/blob/master/source/transactions/transactions.rst#id71)
/// requires not to have a transaction object, so that the user
/// cannot get confused.
class TransactionInfo {
  TransactionInfo({TransactionOptions? options})
      : options = options ?? TransactionOptions();
  TransactionOptions options;
  TransactionState state = TransactionState.none;
  Server? _pinnedServer;
  Int64 transactionNumber = Int64();

  bool get isPinned => _pinnedServer != null;

  bool get isFirstTransaction => state == TransactionState.starting;
  bool get isActive =>
      state == TransactionState.inProgress ||
      state == TransactionState.starting;
  bool get isCommitted =>
      state == TransactionState.committed ||
      state == TransactionState.committedEmpty ||
      state == TransactionState.aborted;

  void pinServer(server) {
    if (isActive) {
      _pinnedServer = server;
    }
  }

  void unpinServer() => _pinnedServer = null;
}
