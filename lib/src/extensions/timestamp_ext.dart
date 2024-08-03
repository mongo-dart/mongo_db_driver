import 'package:bson/bson.dart';

extension TimestampExt on Timestamp {
  bool isAfter(Timestamp other) =>
      seconds > other.seconds ||
      (seconds == other.seconds && increment > other.increment);
}
