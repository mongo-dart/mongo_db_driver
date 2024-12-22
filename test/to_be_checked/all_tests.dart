library;

import 'database_test.dart' as database;
import '../gridfs_test.dart' as gridfs;
import '../packet_converter_test.dart' as converter;
import '../utils_test.dart' as utils_tests;
import 'decimal_test.dart' as decimal;
import '../message_test.dart' as message;
import 'op_msg_bulk_operation_test.dart' as bulk;
import 'op_msg_collection_test.dart' as collection;
import 'op_msg_commands_test.dart' as commands;
import 'op_msg_read_operation_test.dart' as read_op;
import 'op_msg_write_operation_test.dart' as write_op;
import 'mongo_db_query_test.dart' as mongo_db_query_test;

void main() {
  converter.main();
  database.main();
  gridfs.main();
  mongo_db_query_test.main();
  utils_tests.main();
  decimal.main();
  message.main();
  bulk.main();
  collection.main();
  commands.main();
  read_op.main();
  write_op.main();
}
