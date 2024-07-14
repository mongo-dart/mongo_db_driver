import '../../../../command_exp.dart'
    show UpdateOneOperation, UpdateOneOptionsOpen, UpdateOneStatementOpen;

base class UpdateOneOperationOpen extends UpdateOneOperation {
  UpdateOneOperationOpen(
      super.collection, UpdateOneStatementOpen super.updateOneStatement,
      {super.session,
      UpdateOneOptionsOpen? super.updateOneOptions,
      super.rawOptions})
      : super.protected();
}
