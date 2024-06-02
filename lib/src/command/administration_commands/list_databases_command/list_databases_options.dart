import 'package:mongo_db_driver/src/command/base/operation_base.dart';
import 'package:mongo_db_driver/src/utils/map_keys.dart';
import 'package:mongo_db_query/mongo_db_query.dart';

import '../../../unions/query_union.dart';

/// ListDatabases command options;
class ListDatabasesOptions {
  /// Optional. A query predicate that determines which databases are listed.
  /// You can specify a condition on any of the fields in the output of
  /// listDatabases:
  ///
  /// - name
  /// - sizeOnDisk
  /// - empty
  /// - shards
  final MongoDocument filter;

  /// Optional. A flag to indicate whether the command should return just the
  /// database names, or return both database names and size information.
  /// Returning size information requires locking each database one at a time,
  /// while returning only names does not require locking any database.
  /// The default value is false, so listDatabases returns the name and size
  /// information of each database.
  final bool nameOnly;

  /// Optional. A flag that determines which databases are returned based on
  /// the user privileges when access control is enabled.
  /// If authorizedDatabases is unspecified, and
  ///   - If the user has listDatabases action on the cluster resource,
  ///      listDatabases command returns all databases.
  ///   - If the user does not have listDatabases action on the cluster:
  ///       * For MongoDB 4.0.6+, listDatabases command returns only the
  /// databases for which the user has privileges (including databases for
  /// which the user has privileges on specific collections).
  ///       * For MongoDB 4.0.5, listDatabases command returns only the
  /// databases for which the user has the find action on the database
  /// resource (and not the collection resource).
  /// If authorizedDatabases is true,
  ///   -  For MongoDB 4.0.6+, listDatabases command returns only the
  /// databases for which the user has privileges (including databases for
  /// which the user has privileges on specific collections).
  ///   -  For MongoDB 4.0.5, listDatabases command returns only the databases
  /// for which the user has the find action on the database resource
  /// (and not the collection resource).
  /// If authorizedDatabases is false, and
  ///   -  If the user has listDatabases action on the cluster, listDatabases
  /// command returns all databases
  ///   -  If the user does not have listDatabases action on the cluster,
  /// listDatabases command errors with insufficient permissions.
  final bool? authorizedDatabases;

  /// A user-provided comment to attach to this command. Once set,
  /// this comment appears alongside records of this command in the following
  /// locations:
  /// - mongod log messages, in the attr.command.cursor.comment field.
  /// - Database profiler output, in the command.comment field.
  /// - currentOp output, in the command.comment field.
  /// We limit Comment to String only
  final String? comment;

  ListDatabasesOptions(
      {dynamic listFilter,
      this.nameOnly = false,
      this.authorizedDatabases,
      this.comment})
      : filter = QueryUnion(listFilter).query;

  Options get options => <String, dynamic>{
        if (filter.isNotEmpty) keyFilter: filter,
        if (nameOnly) keyNameOnly: nameOnly,
        if (authorizedDatabases != null)
          keyAuthorizedDatabases: authorizedDatabases,
        if (comment != null) keyComment: comment!,
      };
}
