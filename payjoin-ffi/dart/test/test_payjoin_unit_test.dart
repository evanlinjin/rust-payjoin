import 'dart:typed_data';
import 'package:convert/convert.dart';
import 'package:test/test.dart';
import "package:payjoin/payjoin.dart" as payjoin;
import "utils.dart";

const String _ohttpKeysHex =
    "01001604ba48c49c3d4a92a3ad00ecc63a024da10ced02180c73ec12d8a7ad2cc91bb483824fe2bee8d28bfe2eb2fc6453bc4d31cd851e8a6540e86c5382af588d370957000400010003";

payjoin.OhttpKeys _ohttpKeys() => payjoin.OhttpKeys.decode(
  bytes: Uint8List.fromList(hex.decode(_ohttpKeysHex)),
);

class _BuiltReceiver {
  final payjoin.Initialized initialized;
  final payjoin.ReceiverEventBuffer buf;
  _BuiltReceiver(this.initialized, this.buf);
}

/// Build an `Initialized` receiver: stage into a buffer, drain, confirm.
_BuiltReceiver _buildReceiver(
  String address,
  InMemoryReceiverPersister persister,
) {
  final buf = payjoin.ReceiverEventBuffer();
  final provisional = payjoin.ReceiverBuilder(
    address: address,
    directory: "https://example.com",
    ohttpKeys: _ohttpKeys(),
  ).build(buf: buf);
  persister.drain(buf);
  return _BuiltReceiver(provisional.confirm(buf: buf), buf);
}

Future<_BuiltReceiver> _buildReceiverAsync(
  String address,
  InMemoryReceiverPersisterAsync persister,
) async {
  final buf = payjoin.ReceiverEventBuffer();
  final provisional = payjoin.ReceiverBuilder(
    address: address,
    directory: "https://example.com",
    ohttpKeys: _ohttpKeys(),
  ).build(buf: buf);
  await persister.drain(buf);
  return _BuiltReceiver(provisional.confirm(buf: buf), buf);
}

void main() {
  group('Test URIs', () {
    test('Test todo url encoded', () {
      var uri =
          "bitcoin:12c6DSiU4Rq3P4ZxziKxzrL5LmMBrzjrJX?amount=1&pj=https://example.com?ciao";
      final result = payjoin.Url.parse(input: uri);
      expect(
        result,
        isA<payjoin.Url>(),
        reason: "pj url should be url encoded",
      );
    });

    test('Test valid url', () {
      var uri =
          "bitcoin:12c6DSiU4Rq3P4ZxziKxzrL5LmMBrzjrJX?amount=1&pj=https://example.com?ciao";
      final result = payjoin.Url.parse(input: uri);
      expect(result, isA<payjoin.Url>(), reason: "pj is not a valid url");
    });

    test('Test missing amount', () {
      var uri =
          "bitcoin:12c6DSiU4Rq3P4ZxziKxzrL5LmMBrzjrJX?pj=https://testnet.demo.btcpayserver.org/BTC/pj";
      final result = payjoin.Url.parse(input: uri);
      expect(result, isA<payjoin.Url>(), reason: "missing amount should be ok");
    });

    test('Test valid uris', () {
      final https = payjoin.exampleUrl();
      final onion =
          "http://vjdpwgybvubne5hda6v4c5iaeeevhge6jvo3w2cl6eocbwwvwxp7b7qd.onion";

      final base58 = "bitcoin:12c6DSiU4Rq3P4ZxziKxzrL5LmMBrzjrJX";
      final bech32Upper = "BITCOIN:TB1Q6D3A2W975YNY0ASUVD9A67NER4NKS58FF0Q8G4";
      final bech32Lower = "bitcoin:tb1q6d3a2w975yny0asuvd9a67ner4nks58ff0q8g4";

      final addresses = [base58, bech32Upper, bech32Lower];
      final pjs = [https, onion];

      for (final address in addresses) {
        for (final pj in pjs) {
          final uri = "$address?amount=1&pj=$pj";
          try {
            payjoin.Url.parse(input: uri);
          } catch (e) {
            fail("Failed to create a valid Uri for $uri. Error: $e");
          }
        }
      }
    });
  });

  group("Test Persistence", () {
    test("Test receiver persistence", () {
      final persister = InMemoryReceiverPersister();
      _buildReceiver("tb1q6d3a2w975yny0asuvd9a67ner4nks58ff0q8g4", persister);

      final result = payjoin.replayReceiverEventLog(events: persister.load());
      expect(
        result.state(),
        isA<payjoin.InitializedReceiveSession>(),
        reason: "receiver should be in Initialized state",
      );
    });

    test("Test sender persistence", () {
      final recvPersister = InMemoryReceiverPersister();
      final built = _buildReceiver(
        "2MuyMrZHkbHbfjudmKUy45dU4P17pjG2szK",
        recvPersister,
      );
      final uri = built.initialized.pjUri();

      final sendPersister = InMemorySenderPersister();
      final sendBuf = payjoin.SenderEventBuffer();
      final psbt = payjoin.originalPsbt();
      payjoin.SenderBuilder(psbt: psbt, uri: uri)
          .buildRecommended(minFeeRateSatPerKwu: BigInt.from(1000), buf: sendBuf);
      sendPersister.drain(sendBuf);

      final senderResult = payjoin.replaySenderEventLog(
        events: sendPersister.load(),
      );
      expect(
        senderResult.state(),
        isA<payjoin.WithReplyKeySendSession>(),
        reason: "sender should be in WithReplyKey state",
      );
    });
  });

  group("Test Receiver Cancel", () {
    test("Test receiver cancel from initialized", () {
      final persister = InMemoryReceiverPersister();
      final built = _buildReceiver(
        "tb1q6d3a2w975yny0asuvd9a67ner4nks58ff0q8g4",
        persister,
      );

      // Receiver cancel returns Optional<bytes> — none for an unused session.
      final fallbackTx = built.initialized.cancel(buf: built.buf);
      persister.drain(built.buf);
      expect(fallbackTx, isNull);

      final result = payjoin.replayReceiverEventLog(events: persister.load());
      expect(
        result.state(),
        isA<payjoin.ClosedReceiveSession>(),
        reason: "receiver should be in Closed state after cancel",
      );
    });

    test("Test receiver cancel async from initialized", () async {
      final persister = InMemoryReceiverPersisterAsync();
      final built = await _buildReceiverAsync(
        "tb1q6d3a2w975yny0asuvd9a67ner4nks58ff0q8g4",
        persister,
      );

      final fallbackTx = built.initialized.cancel(buf: built.buf);
      await persister.drain(built.buf);
      expect(fallbackTx, isNull);

      final events = await persister.load();
      final result = payjoin.replayReceiverEventLog(events: events);
      expect(
        result.state(),
        isA<payjoin.ClosedReceiveSession>(),
        reason: "receiver should be in Closed state after cancel",
      );
    });
  });

  group("Test Sender Cancel", () {
    test("Test sender cancel from with reply key", () {
      final recvPersister = InMemoryReceiverPersister();
      final built = _buildReceiver(
        "2MuyMrZHkbHbfjudmKUy45dU4P17pjG2szK",
        recvPersister,
      );
      final uri = built.initialized.pjUri();

      final sendPersister = InMemorySenderPersister();
      final sendBuf = payjoin.SenderEventBuffer();
      final psbt = payjoin.originalPsbt();
      final withReplyKey = payjoin.SenderBuilder(psbt: psbt, uri: uri)
          .buildRecommended(
            minFeeRateSatPerKwu: BigInt.from(1000),
            buf: sendBuf,
          );
      sendPersister.drain(sendBuf);

      // Sender cancel always returns the fallback tx as raw bytes.
      final fallbackTx = withReplyKey.cancel(buf: sendBuf);
      sendPersister.drain(sendBuf);
      expect(fallbackTx, isNotNull);
      expect(fallbackTx.length, greaterThan(0));

      final result = payjoin.replaySenderEventLog(events: sendPersister.load());
      expect(
        result.state(),
        isA<payjoin.ClosedSendSession>(),
        reason: "sender should be in Closed state after cancel",
      );
    });

    test("Test sender cancel async from with reply key", () async {
      final recvPersister = InMemoryReceiverPersisterAsync();
      final built = await _buildReceiverAsync(
        "2MuyMrZHkbHbfjudmKUy45dU4P17pjG2szK",
        recvPersister,
      );
      final uri = built.initialized.pjUri();

      final sendPersister = InMemorySenderPersisterAsync();
      final sendBuf = payjoin.SenderEventBuffer();
      final psbt = payjoin.originalPsbt();
      final withReplyKey = payjoin.SenderBuilder(psbt: psbt, uri: uri)
          .buildRecommended(
            minFeeRateSatPerKwu: BigInt.from(1000),
            buf: sendBuf,
          );
      await sendPersister.drain(sendBuf);

      final fallbackTx = withReplyKey.cancel(buf: sendBuf);
      await sendPersister.drain(sendBuf);
      expect(fallbackTx, isNotNull);
      expect(fallbackTx.length, greaterThan(0));

      final events = await sendPersister.load();
      final result = payjoin.replaySenderEventLog(events: events);
      expect(
        result.state(),
        isA<payjoin.ClosedSendSession>(),
        reason: "sender should be in Closed state after cancel",
      );
    });
  });

  group("Test Async Persistence", () {
    test("Test receiver async persistence", () async {
      final persister = InMemoryReceiverPersisterAsync();
      await _buildReceiverAsync(
        "tb1q6d3a2w975yny0asuvd9a67ner4nks58ff0q8g4",
        persister,
      );
      final events = await persister.load();
      final result = payjoin.replayReceiverEventLog(events: events);
      expect(
        result.state(),
        isA<payjoin.InitializedReceiveSession>(),
        reason: "receiver should be in Initialized state",
      );
    });

    test("Test sender async persistence", () async {
      final recvPersister = InMemoryReceiverPersisterAsync();
      final built = await _buildReceiverAsync(
        "2MuyMrZHkbHbfjudmKUy45dU4P17pjG2szK",
        recvPersister,
      );
      final uri = built.initialized.pjUri();

      final sendPersister = InMemorySenderPersisterAsync();
      final sendBuf = payjoin.SenderEventBuffer();
      final psbt = payjoin.originalPsbt();
      payjoin.SenderBuilder(psbt: psbt, uri: uri).buildRecommended(
        minFeeRateSatPerKwu: BigInt.from(1000),
        buf: sendBuf,
      );
      await sendPersister.drain(sendBuf);

      final events = await sendPersister.load();
      final senderResult = payjoin.replaySenderEventLog(events: events);
      expect(
        senderResult.state(),
        isA<payjoin.WithReplyKeySendSession>(),
        reason: "sender should be in WithReplyKey state",
      );
    });

    test("Validation sender builder rejects bad psbt", () {
      final uri = payjoin.Uri.parse(
        uri:
            "bitcoin:tb1q6d3a2w975yny0asuvd9a67ner4nks58ff0q8g4?pj=https://example.com/pj",
      ).checkPjSupported();
      expect(
        () => payjoin.SenderBuilder(psbt: "not-a-psbt", uri: uri),
        throwsA(isA<payjoin.SenderInputException>()),
      );
    });
  });
}
