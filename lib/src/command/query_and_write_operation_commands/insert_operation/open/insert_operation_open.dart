import '../base/insert_operation.dart';

base class InsertOperationOpen extends InsertOperation {
  InsertOperationOpen(super.collection, super.documents,
      {super.session, super.insertOptions, super.rawOptions})
      : super.protected();
}
