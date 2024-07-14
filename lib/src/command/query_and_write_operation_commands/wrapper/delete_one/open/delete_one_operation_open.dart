import '../../../../command_exp.dart'
    show DeleteOneOperation, DeleteOneOptionsOpen, DeleteOneStatementOpen;

base class DeleteOneOperationOpen extends DeleteOneOperation {
  DeleteOneOperationOpen(
      super.collection, DeleteOneStatementOpen super.deleteOneStatement,
      {super.session,
      DeleteOneOptionsOpen? super.deleteOneOptions,
      super.rawOptions})
      : super.protected();
}
