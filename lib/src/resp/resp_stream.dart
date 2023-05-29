import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:redis_dart/redis_dart.dart';

/// Represents a stream that implements the RESP[1] protocol
/// [1] https://redis.io/docs/reference/protocol-spec/
///
/// To interact with the server use the `write` and/or `readValue` methods.
///
/// Example:
/// ```dart
/// final stream = RespStream(Socket.connect('localhost', 6379));
///
/// // write commands to server
/// stream.write(['SET', 'key1', 'value1']);
///
/// // read from server (blocks until server sends data)
/// final result = stream.readValue(); // 'OK' (type of String)
///
/// ```
class RespStream {
  static final _emptyList = Uint8List.fromList([]);

  final StreamIterator<Uint8List> _input;
  final StreamSink<List<int>> _output;

  Uint8List _buffer = _emptyList;

  /// Creates a new RespStream with given [socket]
  RespStream(Socket socket)
      : _input = StreamIterator(socket),
        _output = socket;

  /// Encodes arguments and writes to the server
  ///
  /// Usage:
  /// ```dart
  /// stream.write(['GET', 'key1']);
  /// ```
  void write(List<Object> arguments) {
    _output.add(RespEncoder.encode(arguments));
  }

  /// Decodes server response stream into objects (e.g Strings, integers, Lists & Errors).
  ///
  /// This operation blocks until server sends a valid response
  Future<Object?> readValue() async {
    return RespDecoder.decode(this);
  }

  /// Reads one (1) byte from stream.
  Future<int?> readByte() async {
    final bytes = await readBytes(1);
    return bytes.isNotEmpty ? bytes[0] : null;
  }

  /// Reads (n) bytes from stream.
  Future<Uint8List> readBytes(int size) async {
    final out = BytesBuilder(copy: false);

    while (size > 0) {
      if (_buffer.isEmpty) {
        if (!(await _input.moveNext())) {
          // Don't attempt to read more data, as there is no more data.
          break;
        }
        _buffer = _input.current;
      }

      if (_buffer.isNotEmpty) {
        if (size < _buffer.length) {
          out.add(Uint8List.sublistView(_buffer, 0, size));
          _buffer = Uint8List.sublistView(_buffer, size);
          break;
        }

        out.add(_buffer);
        size -= _buffer.length;
        _buffer = _emptyList;
      }
    }

    return out.toBytes();
  }

  /// Reads bytes from stream until a newline `\r\n` is reached.
  Future<Uint8List> readLine() async {
    final out = BytesBuilder(copy: false);

    while (true) {
      if (_buffer.isEmpty) {
        if (!(await _input.moveNext())) {
          // Don't attempt to read more data, as there is no more data.
          break;
        }
        _buffer = _input.current;
      }

      if (_buffer.isNotEmpty) {
        final i = _buffer.indexOf('\n'.codeUnitAt(0));
        if (i != -1) {
          out.add(Uint8List.sublistView(_buffer, 0, i + 1));
          _buffer = Uint8List.sublistView(_buffer, i + 1);
          break;
        }

        out.add(_buffer);
        _buffer = _emptyList;
      }
    }

    return out.toBytes();
  }

  /// Cancel writing to server.
  Future<dynamic> cancel() async {
    return _input.cancel();
  }

  /// Cancels listening to server output.
  Future<void> close() async {
    return _output.close().catchError((_) {/* ignore */});
  }
}
