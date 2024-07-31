import '../../database/database_exp.dart' show MongoCollection, MongoDatabase;
import '../../client/client_exp.dart'
    show
        MongoClient,
        MongoDartError,
        key$ReadPreference,
        keyEnabled,
        keyHedgeOptions,
        keyMaxStalenessSecond,
        keyMode,
        keyReadPreference,
        keyReadPreferenceTags,
        keyTags;
import '../base/operation_base.dart';

typedef TagSet = Map<String, String>;

enum ReadPreferenceMode {
  primary('primary'),
  primaryPreferred('primaryPreferred'),
  secondary('secondary'),
  secondaryPreferred('secondaryPreferred'),
  nearest('nearest');

  const ReadPreferenceMode(this.name);
  final String name;
}

var readPrefernceKeys = [
  key$ReadPreference,
  keyReadPreference,
  keyReadPreferenceTags,
  keyMaxStalenessSecond,
  keyHedgeOptions
];

ReadPreferenceMode getReadPreferenceModeFromString(String mode) =>
    ReadPreferenceMode.values.firstWhere((element) => element.name == mode);

///
/// The **ReadPreference** class is a class that represents a MongoDB
/// ReadPreference and is used to construct connections.
///  @class
/// mode A string describing the read preference mode (primary|primaryPreferred|secondary|secondaryPreferred|nearest)
///  tags The tags object
/// @see https://docs.mongodb.com/manual/core/read-preference/
/// @return {ReadPreference}
class ReadPreference {
  ReadPreference(this.mode,
      {this.tags, this.maxStalenessSeconds, this.hedgeOptions}) {
    if (mode == ReadPreferenceMode.primary) {
      if (tags != null && tags!.isNotEmpty) {
        if (tags!.length > 1 || tags!.first.isNotEmpty) {
          throw ArgumentError(
              'Primary read preference cannot be combined with tags');
        }
      }
      if (maxStalenessSeconds != null) {
        throw ArgumentError(
            'Primary read preference cannot be combined with maxStalenessSeconds');
      }
      if (hedgeOptions != null) {
        throw ArgumentError('Primary read preference cannot set hedge options');
      }
    }
    if (maxStalenessSeconds != null && maxStalenessSeconds! < 0) {
      throw ArgumentError('maxStalenessSeconds must be a positive integer');
    }
  }

  static const secondaryOK = [
    ReadPreferenceMode.primaryPreferred,
    ReadPreferenceMode.secondary,
    ReadPreferenceMode.secondaryPreferred,
    ReadPreferenceMode.nearest
  ];

  static ReadPreference primary = ReadPreference(ReadPreferenceMode.primary);
  static ReadPreference primaryPreferred =
      ReadPreference(ReadPreferenceMode.primaryPreferred);
  static ReadPreference secondary =
      ReadPreference(ReadPreferenceMode.secondary);
  static ReadPreference secondaryPreferred =
      ReadPreference(ReadPreferenceMode.secondaryPreferred);
  static ReadPreference nearest = ReadPreference(ReadPreferenceMode.nearest,
      hedgeOptions: {keyEnabled: true});

  /// Default choice as per [specifications](https://github.com/mongodb/specifications/blob/master/source/server-selection/server-selection.rst#components-of-a-read-preference)
  static ReadPreference preferenceDefault = ReadPreference(
      ReadPreferenceMode.primary,
      tags: <Map<String, String>>[<String, String>{}]);

  static int? getMaxStalenessSeconds(Map<String, Object>? options) {
    if (options == null) {
      return null;
    }
    if (options[keyMaxStalenessSecond] != null) {
      if (options[keyMaxStalenessSecond] is! int ||
          options[keyMaxStalenessSecond] as int < 0) {
        throw ArgumentError('maxStalenessSeconds must be a positive integer');
      }
      return options[keyMaxStalenessSecond] as int;
    }
    return null;
  }

  final ReadPreferenceMode mode;
  final List<TagSet>? tags;
  final int? maxStalenessSeconds;
  final Map<String, Object>? hedgeOptions;

  /// We can accept three formats for ReadPreference inside Options:
  /// - options[keyReadPreference] id ReadPreference
  ///    an Instance of ReadPreference)
  /// - options[keyReadPrefernce] is Map (in format:
  ///    {keyMode: <String>,
  ///     keyReadPrefernceTags: <List>,
  ///     keyMaxStalenessSeconds: <int>,
  ///     keyHedgedOptions:  {'enabled' : true/false}
  ///    })
  /// - options[keyReadPreference] is ReadPreferenceMode.
  ///   In this this case we expect the other options to be inside the options
  ///   map itself (ex. options[keyReadPreferencTags])
  ///
  factory ReadPreference.fromOptions(Options options,
      {bool? removeFromOriginalMap}) {
    if (options[keyReadPreference] == null) {
      throw MongoDartError('ReadPreference mode is needed');
    }
    var remove = removeFromOriginalMap ?? false;
    dynamic readPreference =
        remove ? options.remove(keyReadPreference) : options[keyReadPreference];
    if (readPreference is ReadPreferenceMode) {
      return ReadPreference(readPreference,
          tags: (remove
              ? options.remove(keyReadPreferenceTags)
              : options[keyReadPreferenceTags]) as List<TagSet>?,
          maxStalenessSeconds: (remove
              ? options.remove(keyMaxStalenessSecond)
              : options[keyMaxStalenessSecond]) as int?,
          hedgeOptions: (remove
              ? options.remove(keyHedgeOptions)
              : options[keyHedgeOptions]) as Map<String, Object>?);
    } else if (readPreference is Map) {
      var mode = readPreference[keyMode] as String?;
      if (mode != null) {
        return ReadPreference(getReadPreferenceModeFromString(mode),
            tags: (remove
                ? readPreference.remove(keyReadPreferenceTags)
                : readPreference[keyReadPreferenceTags]) as List<TagSet>?,
            maxStalenessSeconds: (remove
                ? readPreference.remove(keyMaxStalenessSecond)
                : readPreference[keyMaxStalenessSecond]) as int?,
            hedgeOptions: (remove
                ? readPreference.remove(keyHedgeOptions)
                : readPreference[keyHedgeOptions]) as Map<String, Object>?);
      }
    } else if (options[keyReadPreference] is ReadPreference) {
      return options[keyReadPreference] as ReadPreference;
    }
    throw UnsupportedError('The "$keyReadPreference" value is of an '
        'unmanaged type ${options[keyReadPreference].runtimeType}');
  }

  // As in Dart mode is enum, the value is always valid
  /* static bool isValid(ReadPreferenceMode mode) => true; */

  ///
  /// Indicates that this readPreference needs the "slaveOk" bit when sent over the wire
  /// @method
  /// @return {boolean}
  /// @see https://docs.mongodb.com/manual/reference/mongodb-wire-protocol/#op-query
  bool get secondaryOk => secondaryOK.contains(mode);

  @override
  bool operator ==(other) => other is ReadPreference && mode == other.mode;

  @override
  int get hashCode => mode.hashCode;

  Options toMap() => <String, dynamic>{
        key$ReadPreference: {
          keyMode: mode.name,
          if (tags != null) keyTags: tags!,
          if (maxStalenessSeconds != null)
            keyMaxStalenessSecond: maxStalenessSeconds!,
          if (hedgeOptions != null) keyHedgeOptions: hedgeOptions!
        }
      };

  static void removeReadPreferenceFromOptions(Map<String, dynamic> options) =>
      options.removeWhere((key, value) => readPrefernceKeys.contains(key));
}

/// Resolves a read preference based on well-defined inheritance rules.
/// This method will not only determine the read preference (if there is one),
/// but will also ensure the returned value is a properly constructed
/// instance of `ReadPreference`.
///
/// @param {Collection|Db|MongoClient} parent The parent of the operation on
/// which to determine the read preference, used for determining the inherited
/// read preference.
/// @param {Object} options The options passed into the method,
/// potentially containing a read preference
ReadPreference? resolveReadPreference(parent,
    {Options? options, bool? inheritReadPreference = true}) {
  options ??= <String, dynamic>{};
  inheritReadPreference ??= true;

  if (options[keyReadPreference] != null) {
    return ReadPreference.fromOptions(options);
  } // Todo session Class not yet implemented
  /*else if ((session?.inTransaction() ?? false) && session.transaction.options[CommandOperation.keyReadPreference]) {
    // The transaction’s read preference MUST override all other user configurable read preferences.
    readPreference = session.transaction.options[CommandOperation.keyReadPreference];
  }*/

  ReadPreference? inheritedReadPreference;

  if (inheritReadPreference) {
    if (parent is MongoCollection) {
      inheritedReadPreference = parent.readPreference ??
          parent.db.readPreference ??
          parent.db.mongoClient.mongoClientOptions.readPreference;
    } else if (parent is MongoDatabase) {
      inheritedReadPreference = parent.readPreference ??
          parent.mongoClient.mongoClientOptions.readPreference;
    } else if (parent is MongoClient) {
      inheritedReadPreference = parent.mongoClientOptions.readPreference;
    }
    if (inheritedReadPreference == null) {
      throw MongoDartError('No readPreference was provided or inherited.');
    }
  }

  return inheritedReadPreference;
}
