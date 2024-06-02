import '../../../../database/base/mongo_database.dart';
import '../../create_command/create_command.dart';
import 'create_collection_options.dart';

/// createCollection command.
///
/// Explicitly creates a collection.
///
/// Because MongoDB creates a collection implicitly when the collection is
/// first referenced in a command, this method is used primarily for creating
/// new collections that use specific options. For example,
/// you use db.createCollection() to create a capped collection, or to create
/// a new collection that uses document validation.
///
/// **Starting in MongoDB 4.2**
///
/// MongoDB removes the MMAPv1 storage engine and the MMAPv1 specific options
/// `paddingFactor`, `paddingBytes`, `preservePadding` for
/// `db.createCollection()`.
///
/// **Note**
///
/// The Original shell command allows to create also views.
/// We restrict this behavior and you will need the `db.createView()` (or
/// directly the `CreateCommand`) to create one.
///
///
/// The command accepts the following fields:
/// - db [MongoDatabase]
///   The database on which create the collection
/// - name 	[String]
///   The collection name to be created.
/// - createCollectionOptions [createCollectionOptions] - Optional
///   a set of optional values for the command
/// - rawOption [Map]
///   An alternative way to creteCollectionOptions to specify command options
///   (must be manually set)
base class CreateCollectionCommand extends CreateCommand {
  CreateCollectionCommand(super.db, super.name,
      {CreateCollectionOptions? createCollectionOptions, super.rawOptions})
      : super(createOptions: createCollectionOptions);
}
