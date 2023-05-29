import 'dart:convert';
import 'dart:typed_data';

/// A [RespEncoder] encodes objects to a byte array that can interpreted by a Redis server.
class RespEncoder {
  static final _newLine = ascii.encode('\r\n');
  static final int _array = '*'.codeUnitAt(0);
  static final int _bulkString = r'$'.codeUnitAt(0);

  RespEncoder._();

  /// Encodes [arguments] to a byte array.
  static Uint8List encode(List<Object> arguments) {
    final out = BytesBuilder(copy: false);

    // write array with size (arguments.length)
    out.addByte(_array);
    out.add(ascii.encode(arguments.length.toString()));
    out.add(_newLine);

    for (final arg in arguments) {
      List<int> bytes;

      // encode argument depending of type to byte array
      if (arg is String) {
        bytes = utf8.encode(arg);
      } else if (arg is List<int>) {
        bytes = arg;
      } else if (arg is int || arg is bool) {
        bytes = ascii.encode(arg.toString());
      } else {
        throw ArgumentError.value(
          arguments,
          'arguments',
          'Expected type List<int>, String or int.',
        );
      }

      // write argument as bulk string (string length + string bytes)
      out.addByte(_bulkString);
      out.add(ascii.encode(bytes.length.toString()));
      out.add(_newLine);
      out.add(bytes);
      out.add(_newLine);
    }

    return out.toBytes();
  }
}
