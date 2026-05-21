import unittest
import payjoin
from .utils import (
    InMemoryReceiverPersister,
    InMemoryReceiverPersisterAsync,
    InMemorySenderPersister,
    InMemorySenderPersisterAsync,
)


OHTTP_KEYS_HEX = (
    "01001604ba48c49c3d4a92a3ad00ecc63a024da10ced02180c73ec12d8a7ad2cc91bb4"
    "83824fe2bee8d28bfe2eb2fc6453bc4d31cd851e8a6540e86c5382af588d3709570004"
    "00010003"
)


def _ohttp_keys():
    return payjoin.OhttpKeys.decode(bytes.fromhex(OHTTP_KEYS_HEX))


def _build_receiver(address, persister):
    """Build an `Initialized` receiver: stage into a buffer, drain, confirm."""
    buf = payjoin.ReceiverEventBuffer()
    provisional = payjoin.ReceiverBuilder(
        address, "https://example.com", _ohttp_keys()
    ).build(buf)
    persister.drain(buf)
    return provisional.confirm(buf), buf


async def _build_receiver_async(address, persister):
    buf = payjoin.ReceiverEventBuffer()
    provisional = payjoin.ReceiverBuilder(
        address, "https://example.com", _ohttp_keys()
    ).build(buf)
    await persister.drain(buf)
    return provisional.confirm(buf), buf


class TestURIs(unittest.TestCase):
    def test_todo_url_encoded(self):
        uri = "bitcoin:12c6DSiU4Rq3P4ZxziKxzrL5LmMBrzjrJX?amount=1&pj=https://example.com?ciao"
        self.assertTrue(payjoin.Url.parse(uri), "pj url should be url encoded")

    def test_valid_url(self):
        uri = "bitcoin:12c6DSiU4Rq3P4ZxziKxzrL5LmMBrzjrJX?amount=1&pj=https://example.com?ciao"
        self.assertTrue(payjoin.Url.parse(uri), "pj is not a valid url")

    def test_missing_amount(self):
        uri = "bitcoin:12c6DSiU4Rq3P4ZxziKxzrL5LmMBrzjrJX?pj=https://testnet.demo.btcpayserver.org/BTC/pj"
        self.assertTrue(payjoin.Url.parse(uri), "missing amount should be ok")

    def test_valid_uris(self):
        https = str(payjoin.example_url())
        onion = "http://vjdpwgybvubne5hda6v4c5iaeeevhge6jvo3w2cl6eocbwwvwxp7b7qd.onion"

        base58 = "bitcoin:12c6DSiU4Rq3P4ZxziKxzrL5LmMBrzjrJX"
        bech32_upper = "BITCOIN:TB1Q6D3A2W975YNY0ASUVD9A67NER4NKS58FF0Q8G4"
        bech32_lower = "bitcoin:tb1q6d3a2w975yny0asuvd9a67ner4nks58ff0q8g4"

        for address in [base58, bech32_upper, bech32_lower]:
            for pj in [https, onion]:
                uri = f"{address}?amount=1&pj={pj}"
                try:
                    payjoin.Url.parse(uri)
                except Exception as e:
                    self.fail(f"Failed to create a valid Uri for {uri}. Error: {e}")


class TestReceiverPersistence(unittest.TestCase):
    def test_receiver_persistence(self):
        persister = InMemoryReceiverPersister()
        _build_receiver("tb1q6d3a2w975yny0asuvd9a67ner4nks58ff0q8g4", persister)

        result = payjoin.replay_receiver_event_log(persister.load())
        self.assertTrue(result.state().is_INITIALIZED())


class TestSenderPersistence(unittest.TestCase):
    def test_sender_persistence(self):
        recv_persister = InMemoryReceiverPersister()
        receiver, _ = _build_receiver("2MuyMrZHkbHbfjudmKUy45dU4P17pjG2szK", recv_persister)
        uri = receiver.pj_uri()

        send_persister = InMemorySenderPersister()
        send_buf = payjoin.SenderEventBuffer()
        psbt = payjoin.original_psbt()
        payjoin.SenderBuilder(psbt, uri).build_recommended(1000, send_buf)
        send_persister.drain(send_buf)

        result = payjoin.replay_sender_event_log(send_persister.load())
        self.assertTrue(result.state().is_WITH_REPLY_KEY())


class TestReceiverAsyncPersistence(unittest.TestCase):
    def test_receiver_async_persistence(self):
        import asyncio

        async def run_test():
            persister = InMemoryReceiverPersisterAsync()
            await _build_receiver_async(
                "tb1q6d3a2w975yny0asuvd9a67ner4nks58ff0q8g4", persister
            )
            events = await persister.load()
            result = payjoin.replay_receiver_event_log(events)
            self.assertTrue(result.state().is_INITIALIZED())

        asyncio.run(run_test())


class TestSenderAsyncPersistence(unittest.TestCase):
    def test_sender_async_persistence(self):
        import asyncio

        async def run_test():
            recv_persister = InMemoryReceiverPersisterAsync()
            receiver, _ = await _build_receiver_async(
                "2MuyMrZHkbHbfjudmKUy45dU4P17pjG2szK", recv_persister
            )
            uri = receiver.pj_uri()

            send_persister = InMemorySenderPersisterAsync()
            send_buf = payjoin.SenderEventBuffer()
            psbt = payjoin.original_psbt()
            payjoin.SenderBuilder(psbt, uri).build_recommended(1000, send_buf)
            await send_persister.drain(send_buf)

            events = await send_persister.load()
            result = payjoin.replay_sender_event_log(events)
            self.assertTrue(result.state().is_WITH_REPLY_KEY())

        asyncio.run(run_test())


class TestReceiverCancel(unittest.TestCase):
    def test_receiver_cancel(self):
        persister = InMemoryReceiverPersister()
        initialized, buf = _build_receiver(
            "tb1q6d3a2w975yny0asuvd9a67ner4nks58ff0q8g4", persister
        )

        # cancel pushes a Closed event; no fallback for early-state receiver.
        fallback_tx = initialized.cancel(buf)
        persister.drain(buf)
        self.assertIsNone(fallback_tx)

        result = payjoin.replay_receiver_event_log(persister.load())
        self.assertTrue(result.state().is_CLOSED())


class TestReceiverCancelAsync(unittest.TestCase):
    def test_receiver_cancel_async(self):
        import asyncio

        async def run_test():
            persister = InMemoryReceiverPersisterAsync()
            initialized, buf = await _build_receiver_async(
                "tb1q6d3a2w975yny0asuvd9a67ner4nks58ff0q8g4", persister
            )

            fallback_tx = initialized.cancel(buf)
            await persister.drain(buf)
            self.assertIsNone(fallback_tx)

            events = await persister.load()
            result = payjoin.replay_receiver_event_log(events)
            self.assertTrue(result.state().is_CLOSED())

        asyncio.run(run_test())


class TestSenderCancel(unittest.TestCase):
    def test_sender_cancel(self):
        recv_persister = InMemoryReceiverPersister()
        receiver, _ = _build_receiver(
            "2MuyMrZHkbHbfjudmKUy45dU4P17pjG2szK", recv_persister
        )
        uri = receiver.pj_uri()

        send_persister = InMemorySenderPersister()
        send_buf = payjoin.SenderEventBuffer()
        psbt = payjoin.original_psbt()
        with_reply_key = payjoin.SenderBuilder(psbt, uri).build_recommended(
            1000, send_buf
        )
        send_persister.drain(send_buf)

        # Sender cancel always returns a fallback transaction (raw bytes).
        fallback_tx = with_reply_key.cancel(send_buf)
        send_persister.drain(send_buf)
        self.assertIsNotNone(fallback_tx)
        self.assertTrue(len(fallback_tx) > 0)

        result = payjoin.replay_sender_event_log(send_persister.load())
        self.assertTrue(result.state().is_CLOSED())


class TestSenderCancelAsync(unittest.TestCase):
    def test_sender_cancel_async(self):
        import asyncio

        async def run_test():
            recv_persister = InMemoryReceiverPersisterAsync()
            receiver, _ = await _build_receiver_async(
                "2MuyMrZHkbHbfjudmKUy45dU4P17pjG2szK", recv_persister
            )
            uri = receiver.pj_uri()

            send_persister = InMemorySenderPersisterAsync()
            send_buf = payjoin.SenderEventBuffer()
            psbt = payjoin.original_psbt()
            with_reply_key = payjoin.SenderBuilder(psbt, uri).build_recommended(
                1000, send_buf
            )
            await send_persister.drain(send_buf)

            fallback_tx = with_reply_key.cancel(send_buf)
            await send_persister.drain(send_buf)
            self.assertIsNotNone(fallback_tx)
            self.assertTrue(len(fallback_tx) > 0)

            events = await send_persister.load()
            result = payjoin.replay_sender_event_log(events)
            self.assertTrue(result.state().is_CLOSED())

        asyncio.run(run_test())


class TestValidation(unittest.TestCase):
    def test_receiver_builder_rejects_bad_address(self):
        with self.assertRaises(payjoin.ReceiverBuilderError):
            payjoin.ReceiverBuilder("not-an-address", "https://example.com", _ohttp_keys())

    def test_input_pair_rejects_invalid_outpoint(self):
        with self.assertRaises(payjoin.InputPairError):
            txin = payjoin.TxIn(
                previous_output=payjoin.OutPoint(txid="deadbeef", vout=0),
                script_sig=bytes(),
                sequence=0,
                witness=[],
            )
            psbtin = payjoin.PsbtInput(
                witness_utxo=None, redeem_script=None, witness_script=None
            )
            payjoin.InputPair(txin, psbtin, None)

    def test_sender_builder_rejects_bad_psbt(self):
        uri = payjoin.Uri.parse(
            "bitcoin:tb1q6d3a2w975yny0asuvd9a67ner4nks58ff0q8g4?pj=https://example.com/pj"
        ).check_pj_supported()
        with self.assertRaises(payjoin.SenderInputError):
            payjoin.SenderBuilder("not-a-psbt", uri)


if __name__ == "__main__":
    unittest.main()
