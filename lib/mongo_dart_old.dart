/// Server-side driver library for MongoDb implemented in pure Dart.
/// As most of IO in Dart, mongo_dart is totally async -using Futures and Streams.
/// .

library mongo_dart;

import 'dart:async';
import 'dart:io' show File, FileMode, IOSink, Platform;
import 'dart:typed_data';
import 'package:bson/bson.dart';
import 'package:mongo_db_driver/src/extensions/file_ext.dart';

import 'package:mongo_db_query/mongo_db_query.dart';
import 'package:uuid/uuid.dart';

import 'src/core/error/mongo_dart_error.dart';

import 'src/database/base/mongo_database.dart';
import 'src/database/base/mongo_collection.dart';

import 'package:path/path.dart' as p;

export 'package:bson/bson.dart';
export 'package:mongo_db_query/mongo_db_query.dart';
export 'package:mongo_db_driver/src/command/command.dart';
export 'package:mongo_db_driver/src/utils/map_keys.dart';

//part 'src_old/database/mongo_kill_cursors_message.dart';

part 'src/gridfs/grid_fs_file.dart';

part 'src/gridfs/grid_in.dart';

part 'src/gridfs/grid_out.dart';

part 'src/gridfs/gridfs.dart';

part 'src/gridfs/chunk_handler.dart';
