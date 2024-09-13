// ignore_for_file: deprecated_member_use_from_same_package

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
import '../../topology/abstract/topology.dart';
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

  static ReadPreferenceMode fromString(String mode) =>
      values.firstWhere((element) => element.name == mode);
}

var _readPrefernceKeys = [
  key$ReadPreference,
  keyReadPreference,
  keyReadPreferenceTags,
  keyMaxStalenessSecond,
  keyHedgeOptions
];

/// The **ReadPreference** class is a class that represents a MongoDB
/// ReadPreference and is used to construct connections.
/// see https://docs.mongodb.com/manual/core/read-preference/
class ReadPreference {
  ReadPreference(this.mode,
      {this.tags,
      this.maxStalenessSeconds,
      @Deprecated('since 8.0') Map<String, Object>? hedgeOptions})
      : hedgeOptions = hedgeOptions ??
            (mode == ReadPreferenceMode.nearest ? <String, Object>{} : null) {
    if (mode == ReadPreferenceMode.primary) {
      if (tags != null && tags!.isNotEmpty) {
        if (tags!.length > 1 || tags!.first.isNotEmpty) {
          throw ArgumentError('Read Preference Constructor - '
              'primary cannot be combined with tags');
        }
      }
      if (maxStalenessSeconds != null) {
        throw ArgumentError('Read Preference Constructor - '
            'primary cannot be combined with maxStalenessSeconds');
      }
      if (hedgeOptions != null) {
        throw ArgumentError('Read Preference Constructor - '
            'primary cannot set hedge options');
      }
    }
    // https://www.mongodb.com/docs/manual/core/read-preference-staleness/
    if (maxStalenessSeconds != null && maxStalenessSeconds! < 90) {
      throw ArgumentError('Read Preference Constructor - '
          'maxStalenessSeconds must be at least 90 seconds');
    }
  }

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
    var tagsTemp = (remove
        ? options.remove(keyReadPreferenceTags)
        : options[keyReadPreferenceTags]);
    List<TagSet>? tags = tagsTemp == null
        ? null
        : <TagSet>[
            for (var element in tagsTemp) <String, String>{...?element}
          ];
    int? maxStalenessSeconds = (remove
        ? options.remove(keyMaxStalenessSecond)
        : options[keyMaxStalenessSecond]);
    var hedgeOptionsTemp =
        (remove ? options.remove(keyHedgeOptions) : options[keyHedgeOptions]);
    Map<String, Object>? hedgeOptions =
        hedgeOptionsTemp == null ? null : <String, Object>{...hedgeOptionsTemp};

    if (readPreference is ReadPreference) {
      return ReadPreference(readPreference.mode,
          tags: tags ?? readPreference.tags,
          maxStalenessSeconds:
              maxStalenessSeconds ?? readPreference.maxStalenessSeconds,
          hedgeOptions: hedgeOptions ?? readPreference.hedgeOptions);
    } else if (readPreference is ReadPreferenceMode) {
      return ReadPreference(readPreference,
          tags: tags,
          maxStalenessSeconds: maxStalenessSeconds,
          hedgeOptions: hedgeOptions);
    } else if (readPreference is String) {
      return ReadPreference(ReadPreferenceMode.fromString(readPreference),
          tags: tags,
          maxStalenessSeconds: maxStalenessSeconds,
          hedgeOptions: hedgeOptions);
    } else if (readPreference is Map) {
      var rdMode = readPreference[keyMode];
      var tagsTemp = readPreference[keyReadPreferenceTags];
      var hedgeOptionsTemp = readPreference[keyHedgeOptions];
      if (rdMode is ReadPreferenceMode) {
        return ReadPreference(rdMode,
            tags: tags ??
                (tagsTemp == null
                    ? null
                    : <TagSet>[
                        for (var element in tagsTemp)
                          <String, String>{...?element}
                      ]),
            maxStalenessSeconds:
                maxStalenessSeconds ?? readPreference[keyMaxStalenessSecond],
            hedgeOptions: hedgeOptions ??
                (hedgeOptionsTemp == null
                    ? null
                    : <String, Object>{...hedgeOptionsTemp}));
      } else if (rdMode is String) {
        return ReadPreference(ReadPreferenceMode.fromString(rdMode),
            tags: tags ??
                (tagsTemp == null
                    ? null
                    : <TagSet>[
                        for (var element in tagsTemp)
                          <String, String>{...?element}
                      ]),
            maxStalenessSeconds:
                maxStalenessSeconds ?? readPreference[keyMaxStalenessSecond],
            hedgeOptions: hedgeOptions ??
                (hedgeOptionsTemp == null
                    ? null
                    : <String, Object>{...hedgeOptionsTemp}));
      }
    }
/* 
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
        return ReadPreference(ReadPreferenceMode.fromString(mode),
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
    } */
    throw UnsupportedError('The "$keyReadPreference" value is of an '
        'unmanaged type ${options[keyReadPreference].runtimeType}');
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

  /// Default choice as per
  /// [specifications](https://github.com/mongodb/specifications/blob/master/source/server-selection/server-selection.rst#components-of-a-read-preference)
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

  /// If a replica set member or members are associated with tags, you can
  /// specify a tag set list (array of tag sets) in the read preference to
  /// target those members.
  /// To configure a member with tags, set members[n].tags to a document that
  /// contains the tag name and value pairs. The value of the tags must be a
  /// string.
  ///
  /// { "<tag1>": "<string1>", "<tag2>": "<string2>",... }
  ///
  /// Then, you can include a tag set list in the read preference to target
  /// tagged members. A tag set list is an array of tag sets, where each tag
  /// set contains one or more tag/value pairs.
  ///
  /// [ { "<tag1>": "<string1>", "<tag2>": "<string2>",... }, ... ]
  ///
  /// To find replica set members, MongoDB tries each document in succession
  /// until a match is found.
  final List<TagSet>? tags;

  /// Replica set members can lag behind the primary due to network congestion,
  /// low disk throughput, long-running operations, etc. The read preference
  /// maxStalenessSeconds option lets you specify a maximum replication lag,
  /// or "staleness", for reads from secondaries. When a secondary's estimated
  /// staleness exceeds maxStalenessSeconds, the client stops using it for
  /// read operations.
  ///
  /// Max staleness is not compatible with mode primary and only applies when
  /// selecting a secondary member of a set for a read operation.
  ///
  /// By default, there is no maximum staleness and clients will not consider a
  /// secondary's lag when choosing where to direct a read operation.
  ///
  /// You must specify a maxStalenessSeconds value of 90 seconds or longer:
  /// specifying a smaller maxStalenessSeconds value will raise an error.
  /// Clients estimate secondaries' staleness by periodically checking the
  /// latest write date of each replica set member. Since these checks are
  /// infrequent, the staleness estimate is coarse. Thus, clients cannot
  ///  enforce a maxStalenessSeconds value of less than 90 seconds.
  // TODO Check, write date on Secondary?
  final int? maxStalenessSeconds;

  /// You can specify the use of hedged reads for non-primary read preferences
  /// on sharded clusters.
  ///
  /// With hedged reads, the mongos instances can route read operations to two
  /// replica set members per each queried shard and return results from the
  /// first respondent per shard.
  @Deprecated('since 8.0')
  final Map<String, Object>? hedgeOptions;

  // TODO check if still needed
  bool get secondaryOk => secondaryOK.contains(mode);

  @override
  bool operator ==(other) => other is ReadPreference && mode == other.mode;

  @override
  int get hashCode => mode.hashCode;

  Options toMap({TopologyType topologyType = TopologyType.unknown}) {
    switch (topologyType) {
      case TopologyType.single:
      case TopologyType.unknown:
        return <String, dynamic>{};
      case TopologyType.loadBalanced:
      case TopologyType.sharded:
        return <String, dynamic>{
          key$ReadPreference: {
            keyMode: mode.name,
            if (tags != null) keyTags: tags,
            if (maxStalenessSeconds != null)
              keyMaxStalenessSecond: maxStalenessSeconds,
            if (hedgeOptions != null) keyHedgeOptions: hedgeOptions
          }
        };
      case TopologyType.replicaSetNoPrimary:
      case TopologyType.replicaSetWithPrimary:
        return <String, dynamic>{
          key$ReadPreference: {
            keyMode: mode.name,
            if (tags != null) keyTags: tags,
            if (maxStalenessSeconds != null)
              keyMaxStalenessSecond: maxStalenessSeconds,
          }
        };
    }
  }

  static void removeReadPreferenceFromOptions(Map<String, dynamic> options) =>
      options.removeWhere((key, value) => _readPrefernceKeys.contains(key));
}

/// Resolves a read preference based on well-defined inheritance rules.
/// This method will not only determine the read preference (if there is one),
/// but will also ensure the returned value is a properly constructed
/// instance of `ReadPreference`.
///
/// parent The parent of the operation on
/// which to determine the read preference, used for determining the inherited
/// read preference.
/// options The options passed into the method,
/// potentially containing a read preference
ReadPreference? resolveReadPreference(parent,
    {Options? options, bool? inheritReadPreference = true}) {
  options ??= <String, dynamic>{};
  inheritReadPreference ??= true;

  if (options[keyReadPreference] != null) {
    return ReadPreference.fromOptions(options);
  } // TODO session Class not yet implemented
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
