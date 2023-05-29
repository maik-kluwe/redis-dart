import 'dart:convert';
import 'dart:typed_data';

class RespEncoder {
  static final _newLine = ascii.encode('\r\n');
  static final int _array = '*'.codeUnitAt(0);
  static final int _bulkString = r'$'.codeUnitAt(0);

  RespEncoder._();

  static Uint8List encode(List<Object> arguments) {
    final out = BytesBuilder(copy: false);
    out.addByte(_array);
    out.add(ascii.encode(arguments.length.toString()));
    out.add(_newLine);

    for (final arg in arguments) {
      List<int> bytes;
      if (arg is String) {
        bytes = utf8.encode(arg);
      } else if (arg is List<int>) {
        bytes = arg;
      } else if (arg is int) {
        bytes = ascii.encode(arg.toString());
      } else {
        throw ArgumentError.value(
          arguments,
          'arguments',
          'Expected type List<int>, String or int.',
        );
      }

      out.addByte(_bulkString);
      out.add(ascii.encode(bytes.length.toString()));
      out.add(_newLine);
      out.add(bytes);
      out.add(_newLine);
    }

    return out.toBytes();
  }
}
