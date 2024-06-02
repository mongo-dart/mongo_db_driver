import '../base/insert_operation.dart';

base class InsertOperationV1 extends InsertOperation {
  InsertOperationV1(super.collection, super.documents,
      {super.session, super.insertOptions, super.rawOptions})
      : super.protected();
}
