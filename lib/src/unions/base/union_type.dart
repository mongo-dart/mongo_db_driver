abstract class UnionType<T1, T2> {
  UnionType(dynamic value)
      : valueOne = _assignValue<T1>(value),
        valueTwo = _assignValue<T2>(value);

  dynamic get value => valueOne ?? valueTwo;
  bool get isNull => value == null;

  final T1? valueOne;
  final T2? valueTwo;
}

abstract class MultiUnionType<T1, T2, T3, T4, T5> {
  MultiUnionType(dynamic value)
      : valueOne = _assignValue<T1>(value),
        valueTwo = _assignValue<T2>(value),
        valueThree = _assignValue<T3>(value),
        valueFour = _assignValue<T4>(value),
        valueFive = _assignValue<T5>(value);

  dynamic get value =>
      valueOne ?? valueTwo ?? valueThree ?? valueFour ?? valueFive;
  bool get isNull => value == null;

  final T1? valueOne;
  final T2? valueTwo;
  final T3? valueThree;
  final T4? valueFour;
  final T5? valueFive;
}

abstract class HugeUnionType<T1, T2, T3, T4, T5, T6, T7, T8, T9> {
  HugeUnionType(dynamic value)
      : valueOne = _assignValue<T1>(value),
        valueTwo = _assignValue<T2>(value),
        valueThree = _assignValue<T3>(value),
        valueFour = _assignValue<T4>(value),
        valueFive = _assignValue<T5>(value),
        valueSix = _assignValue<T6>(value),
        valueSeven = _assignValue<T7>(value),
        valueEight = _assignValue<T8>(value),
        valueNine = _assignValue<T9>(value);

  dynamic get value =>
      valueOne ??
      valueTwo ??
      valueThree ??
      valueFour ??
      valueFive ??
      valueSix ??
      valueSeven ??
      valueEight ??
      valueNine;
  bool get isNull => value == null;

  final T1? valueOne;
  final T2? valueTwo;
  final T3? valueThree;
  final T4? valueFour;
  final T5? valueFive;
  final T6? valueSix;
  final T7? valueSeven;
  final T8? valueEight;
  final T9? valueNine;
}

T? _assignValue<T>(value) {
  if (value == null) {
    return null;
  } else if (value is T) {
    return value;
  } else if (value is Map) {
    return _convertMap<T>(value);
  } else if (value is List) {
    return _convertList<T>(value);
  }
  return null;
}

M? _convertMap<M>(Map map) {
  M ret;
  try {
    ret = map as M;
  } catch (e) {
    return null;
  }
  return ret;
}

L? _convertList<L>(List list) {
  L ret;
  try {
    ret = list as L;
  } catch (e) {
    return null;
  }
  return ret;
}
