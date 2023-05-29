import 'dart:convert';
import 'dart:typed_data';

import 'package:redis_dart/redis_dart.dart';

/// A [RespDecoder] decodes a [RespStream] incoming data (byte array) into objects.
class RespDecoder {
  static final int _simpleString = '+'.codeUnitAt(0);
  static final int _integer = ':'.codeUnitAt(0);
  static final int _bulkString = r'$'.codeUnitAt(0);
  static final int _array = '*'.codeUnitAt(0);
  static final int _error = '-'.codeUnitAt(0);

  RespDecoder._();

  /// Decodes the [RespStream] input to an object.
  static Future<Object?> decode(RespStream input) async {
    // read line from server
    Uint8List line = await input.readLine();
    if (line.isEmpty) {
      throw RedisClientException('Incomming stream from server closed');
    }

    final type = line[0];
    final rest = Uint8List.sublistView(line, 1, line.length - 2);

    if (type == _simpleString) {
      return _parseString(rest);
    } else if (type == _integer) {
      return _parseInteger(rest);
    } else if (type == _bulkString) {
      return _parseBulkString(input, rest);
    } else if (type == _array) {
      return _parseArray(input, rest);
    } else if (type == _error) {
      return _parseError(rest);
    }

    return null;
  }

  static int _parseInteger(Uint8List data) {
    int value;

    try {
      value = int.parse(ascii.decode(data));
    } on FormatException catch (e) {
      throw RedisClientException('Invalid integer from server: $e');
    }

    return value;
  }

  static String _parseString(Uint8List data, [bool allowMalformed = false]) {
    try {
      return utf8.decode(
        data,
        allowMalformed: allowMalformed,
      );
    } on FormatException catch (e) {
      throw RedisClientException('Invalid simple string from server: $e');
    }
  }

  static Future<String?> _parseBulkString(RespStream input, Uint8List data) async {
    // read string length (in bytes)
    int length = _parseInteger(data);
    if (length == -1) {
      return null;
    }

    // read string
    Uint8List bytes;
    try {
      bytes = await input.readBytes(length + 2);
    } on Exception catch (e) {
      throw RedisClientException('Exception reading bytes from server: $e');
    }
    return utf8.decode(Uint8List.sublistView(bytes, 0, length));
  }

  static Future<List<Object?>?> _parseArray(RespStream input, Uint8List data) async {
    // read array length (number of items)
    int length = _parseInteger(data);
    if (length == -1) {
      return null;
    }

    // read array objects
    final values = <Object?>[];
    for (var i = 0; i < length; i++) {
      values.add(await decode(input));
    }
    return values;
  }

  static RedisCommandException _parseError(Uint8List data) {
    String type = 'ERR'; // default error type
    String message = _parseString(data, true);

    // try to get specific error type
    final whiteSpaceIdx = message.indexOf(r'\s');
    if (whiteSpaceIdx != -1 && whiteSpaceIdx + 1 < message.length) {
      type = message.substring(0, 1);
      message = message.substring(whiteSpaceIdx + 1);
    }

    return RedisCommandException(type, message);
  }
}
