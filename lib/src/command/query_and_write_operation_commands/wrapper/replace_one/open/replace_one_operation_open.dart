import '../../../../command_exp.dart'
    show ReplaceOneOperation, ReplaceOneOptionsOpen, ReplaceOneStatementOpen;

base class ReplaceOneOperationOpen extends ReplaceOneOperation {
  ReplaceOneOperationOpen(
      super.collection, ReplaceOneStatementOpen super.replaceOneStatement,
      {super.session,
      ReplaceOneOptionsOpen? super.replaceOneOptions,
      super.rawOptions})
      : super.protected();
}
