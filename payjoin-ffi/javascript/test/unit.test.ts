import { describe, test, before } from "node:test";
import assert from "node:assert";
import { payjoin, uniffiInitAsync } from "payjoin";
import * as testUtils from "../test-utils/index.js";
import {
    InMemoryReceiverPersister,
    InMemoryReceiverPersisterAsync,
    InMemorySenderPersister,
    InMemorySenderPersisterAsync,
    parseReceiverEvents,
    parseSenderEvents,
} from "./utils.ts";

before(async () => {
    await uniffiInitAsync();
});

const OHTTP_KEYS_BYTES = new Uint8Array([
    0x01, 0x00, 0x16, 0x04, 0xba, 0x48, 0xc4, 0x9c, 0x3d, 0x4a, 0x92, 0xa3,
    0xad, 0x00, 0xec, 0xc6, 0x3a, 0x02, 0x4d, 0xa1, 0x0c, 0xed, 0x02, 0x18,
    0x0c, 0x73, 0xec, 0x12, 0xd8, 0xa7, 0xad, 0x2c, 0xc9, 0x1b, 0xb4, 0x83,
    0x82, 0x4f, 0xe2, 0xbe, 0xe8, 0xd2, 0x8b, 0xfe, 0x2e, 0xb2, 0xfc, 0x64,
    0x53, 0xbc, 0x4d, 0x31, 0xcd, 0x85, 0x1e, 0x8a, 0x65, 0x40, 0xe8, 0x6c,
    0x53, 0x82, 0xaf, 0x58, 0x8d, 0x37, 0x09, 0x57, 0x00, 0x04, 0x00, 0x01,
    0x00, 0x03,
]).buffer;

function ohttpKeys() {
    return payjoin.OhttpKeys.decode(OHTTP_KEYS_BYTES);
}

/** Build an `Initialized` receiver: stage into a buffer, drain, confirm. */
function buildReceiver(
    address: string,
    persister: InMemoryReceiverPersister,
): { initialized: payjoin.Initialized; buf: payjoin.ReceiverEventBuffer } {
    const buf = payjoin.ReceiverEventBuffer.new();
    const provisional = new payjoin.ReceiverBuilder(
        address,
        "https://example.com",
        ohttpKeys(),
    ).build(buf);
    persister.drain(buf);
    return { initialized: provisional.confirm(buf), buf };
}

async function buildReceiverAsync(
    address: string,
    persister: InMemoryReceiverPersisterAsync,
): Promise<{ initialized: payjoin.Initialized; buf: payjoin.ReceiverEventBuffer }> {
    const buf = payjoin.ReceiverEventBuffer.new();
    const provisional = new payjoin.ReceiverBuilder(
        address,
        "https://example.com",
        ohttpKeys(),
    ).build(buf);
    await persister.drain(buf);
    return { initialized: provisional.confirm(buf), buf };
}

/** Build a `WithReplyKey` sender: stage into a buffer, drain, confirm. */
function buildSender(
    psbt: string,
    uri: payjoin.PjUri,
    persister: InMemorySenderPersister,
    buf: payjoin.SenderEventBuffer,
): payjoin.WithReplyKey {
    const provisional = new payjoin.SenderBuilder(psbt, uri).buildRecommended(
        BigInt(1000),
        buf,
    );
    persister.drain(buf);
    return provisional.confirm(buf);
}

async function buildSenderAsync(
    psbt: string,
    uri: payjoin.PjUri,
    persister: InMemorySenderPersisterAsync,
    buf: payjoin.SenderEventBuffer,
): Promise<payjoin.WithReplyKey> {
    const provisional = new payjoin.SenderBuilder(psbt, uri).buildRecommended(
        BigInt(1000),
        buf,
    );
    await persister.drain(buf);
    return provisional.confirm(buf);
}

describe("URI tests", () => {
    test("URL encoded payjoin parameter", () => {
        const uri =
            "bitcoin:12c6DSiU4Rq3P4ZxziKxzrL5LmMBrzjrJX?amount=1&pj=https://example.com?ciao";
        const result = payjoin.Url.parse(uri);
        assert.ok(result, "pj url should be url encoded");
    });

    test("valid URL", () => {
        const uri =
            "bitcoin:12c6DSiU4Rq3P4ZxziKxzrL5LmMBrzjrJX?amount=1&pj=https://example.com?ciao";
        const result = payjoin.Url.parse(uri);
        assert.ok(result, "pj is not a valid url");
    });

    test("missing amount should be ok", () => {
        const uri =
            "bitcoin:12c6DSiU4Rq3P4ZxziKxzrL5LmMBrzjrJX?pj=https://testnet.demo.btcpayserver.org/BTC/pj";
        const result = payjoin.Url.parse(uri);
        assert.ok(result, "missing amount should be ok");
    });

    test("valid URIs with different addresses and endpoints", () => {
        const https = "https://example.com";
        const onion =
            "http://vjdpwgybvubne5hda6v4c5iaeeevhge6jvo3w2cl6eocbwwvwxp7b7qd.onion";

        const base58 = "bitcoin:12c6DSiU4Rq3P4ZxziKxzrL5LmMBrzjrJX";
        const bech32Upper =
            "BITCOIN:TB1Q6D3A2W975YNY0ASUVD9A67NER4NKS58FF0Q8G4";
        const bech32Lower =
            "bitcoin:tb1q6d3a2w975yny0asuvd9a67ner4nks58ff0q8g4";

        const addresses = [base58, bech32Upper, bech32Lower];
        const pjs = [https, onion];

        for (const address of addresses) {
            for (const pj of pjs) {
                const uri = `${address}?amount=1&pj=${pj}`;
                assert.doesNotThrow(
                    () => payjoin.Url.parse(uri),
                    `Failed to create a valid Uri for ${uri}`,
                );
            }
        }
    });
});

describe("Persistence tests", () => {
    test("receiver persistence", () => {
        const persister = new InMemoryReceiverPersister();
        buildReceiver(
            "tb1q6d3a2w975yny0asuvd9a67ner4nks58ff0q8g4",
            persister,
        );

        const result = payjoin.replayReceiverEventLog(parseReceiverEvents(persister.load()));
        const state = result.state();

        assert.strictEqual(
            state.tag,
            "Initialized",
            "State should be Initialized",
        );
    });

    test("sender persistence", () => {
        const recvPersister = new InMemoryReceiverPersister();
        const { initialized } = buildReceiver(
            "2MuyMrZHkbHbfjudmKUy45dU4P17pjG2szK",
            recvPersister,
        );
        const uri = initialized.pjUri();

        const senderPersister = new InMemorySenderPersister();
        const senderBuf = payjoin.SenderEventBuffer.new();
        const psbt = testUtils.originalPsbt();
        const withReplyKey = buildSender(psbt, uri, senderPersister, senderBuf);

        assert.ok(withReplyKey, "Sender should be created successfully");

        const result = payjoin.replaySenderEventLog(parseSenderEvents(senderPersister.load()));
        assert.strictEqual(result.state().tag, "WithReplyKey");
    });
});

describe("Receiver cancel tests", () => {
    test("receiver cancel from initialized", () => {
        const persister = new InMemoryReceiverPersister();
        const { initialized, buf } = buildReceiver(
            "tb1q6d3a2w975yny0asuvd9a67ner4nks58ff0q8g4",
            persister,
        );

        // Receiver cancel returns Optional<bytes> — none for an unused session.
        const fallbackTx = initialized.cancel(buf);
        persister.drain(buf);
        assert.strictEqual(fallbackTx, undefined);

        const result = payjoin.replayReceiverEventLog(parseReceiverEvents(persister.load()));
        assert.strictEqual(
            result.state().tag,
            "Closed",
            "State should be Closed after cancel",
        );
    });

    test("receiver cancel async from initialized", async () => {
        const persister = new InMemoryReceiverPersisterAsync();
        const { initialized, buf } = await buildReceiverAsync(
            "tb1q6d3a2w975yny0asuvd9a67ner4nks58ff0q8g4",
            persister,
        );

        const fallbackTx = initialized.cancel(buf);
        await persister.drain(buf);
        assert.strictEqual(fallbackTx, undefined);

        const events = await persister.load();
        const result = payjoin.replayReceiverEventLog(parseReceiverEvents(events));
        assert.strictEqual(
            result.state().tag,
            "Closed",
            "State should be Closed after cancel",
        );
    });
});

describe("Sender cancel tests", () => {
    test("sender cancel from with reply key", () => {
        const recvPersister = new InMemoryReceiverPersister();
        const { initialized } = buildReceiver(
            "2MuyMrZHkbHbfjudmKUy45dU4P17pjG2szK",
            recvPersister,
        );
        const uri = initialized.pjUri();

        const senderPersister = new InMemorySenderPersister();
        const senderBuf = payjoin.SenderEventBuffer.new();
        const psbt = testUtils.originalPsbt();
        const withReplyKey = buildSender(psbt, uri, senderPersister, senderBuf);

        // Sender cancel always returns the fallback tx as raw bytes.
        const fallbackTx = withReplyKey.cancel(senderBuf);
        senderPersister.drain(senderBuf);
        assert.ok(fallbackTx, "fallback tx should be returned");
        assert.ok(
            fallbackTx.byteLength > 0,
            "fallback tx bytes should be non-empty",
        );

        const result = payjoin.replaySenderEventLog(parseSenderEvents(senderPersister.load()));
        assert.strictEqual(
            result.state().tag,
            "Closed",
            "State should be Closed after cancel",
        );
    });

    test("sender cancel async from with reply key", async () => {
        const recvPersister = new InMemoryReceiverPersisterAsync();
        const { initialized } = await buildReceiverAsync(
            "2MuyMrZHkbHbfjudmKUy45dU4P17pjG2szK",
            recvPersister,
        );
        const uri = initialized.pjUri();

        const senderPersister = new InMemorySenderPersisterAsync();
        const senderBuf = payjoin.SenderEventBuffer.new();
        const psbt = testUtils.originalPsbt();
        const withReplyKey = await buildSenderAsync(
            psbt,
            uri,
            senderPersister,
            senderBuf,
        );

        const fallbackTx = withReplyKey.cancel(senderBuf);
        await senderPersister.drain(senderBuf);
        assert.ok(fallbackTx, "fallback tx should be returned");
        assert.ok(
            fallbackTx.byteLength > 0,
            "fallback tx bytes should be non-empty",
        );

        const events = await senderPersister.load();
        const result = payjoin.replaySenderEventLog(parseSenderEvents(events));
        assert.strictEqual(
            result.state().tag,
            "Closed",
            "State should be Closed after cancel",
        );
    });
});

describe("Async Persistence tests", () => {
    test("receiver async persistence", async () => {
        const persister = new InMemoryReceiverPersisterAsync();
        await buildReceiverAsync(
            "tb1q6d3a2w975yny0asuvd9a67ner4nks58ff0q8g4",
            persister,
        );

        const events = await persister.load();
        const result = payjoin.replayReceiverEventLog(parseReceiverEvents(events));
        const state = result.state();

        assert.strictEqual(
            state.tag,
            "Initialized",
            "State should be Initialized",
        );
    });

    test("sender async persistence", async () => {
        const recvPersister = new InMemoryReceiverPersisterAsync();
        const { initialized } = await buildReceiverAsync(
            "2MuyMrZHkbHbfjudmKUy45dU4P17pjG2szK",
            recvPersister,
        );
        const uri = initialized.pjUri();

        const senderPersister = new InMemorySenderPersisterAsync();
        const senderBuf = payjoin.SenderEventBuffer.new();
        const psbt = testUtils.originalPsbt();
        const withReplyKey = await buildSenderAsync(
            psbt,
            uri,
            senderPersister,
            senderBuf,
        );

        assert.ok(withReplyKey, "Sender should be created successfully");
    });
});

describe("Validation", () => {
    test("receiver builder rejects bad address", () => {
        assert.throws(() => {
            new payjoin.ReceiverBuilder(
                "not-an-address",
                "https://example.com",
                ohttpKeys(),
            );
        });
    });

    test("input pair rejects invalid outpoint", () => {
        assert.throws(() => {
            const txin = payjoin.TxIn.create({
                previousOutput: payjoin.OutPoint.create({
                    txid: "deadbeef",
                    vout: 0,
                }),
                scriptSig: new Uint8Array([]),
                sequence: 0,
                witness: [],
            });
            const psbtIn = payjoin.PsbtInput.create({
                witnessUtxo: undefined,
                redeemScript: undefined,
                witnessScript: undefined,
            });
            new payjoin.InputPair(txin, psbtIn, undefined);
        });
    });

    test("sender builder rejects bad psbt", () => {
        assert.throws(() => {
            new payjoin.SenderBuilder(
                "not-a-psbt",
                "bitcoin:12c6DSiU4Rq3P4ZxziKxzrL5LmMBrzjrJX",
            );
        });
    });
});

describe("ProvisionalConfirmError variants", () => {
    // The three ProvisionalConfirmError variants must each be reachable
    // through the FFI layer so foreign code can discriminate.
    function stageProvisional(): {
        provisional: payjoin.ProvisionalInitialized;
        buf: payjoin.ReceiverEventBuffer;
    } {
        const buf = payjoin.ReceiverEventBuffer.new();
        const provisional = new payjoin.ReceiverBuilder(
            "tb1q6d3a2w975yny0asuvd9a67ner4nks58ff0q8g4",
            "https://example.com",
            ohttpKeys(),
        ).build(buf);
        return { provisional, buf };
    }

    test("confirm before drain returns NotYetPersisted", () => {
        const { provisional, buf } = stageProvisional();
        // Drain has NOT happened — committed_count == 0, stamp.seq == 1.
        assert.throws(
            () => provisional.confirm(buf),
            (err: Error) =>
                err instanceof payjoin.ProvisionalConfirmError.NotYetPersisted,
        );
    });

    test("confirm against fresh buffer returns WrongBuffer", () => {
        const { provisional, buf } = stageProvisional();
        const persister = new InMemoryReceiverPersister();
        persister.drain(buf); // commit so original buffer WOULD confirm
        const unrelated = payjoin.ReceiverEventBuffer.new();
        assert.throws(
            () => provisional.confirm(unrelated),
            (err: Error) =>
                err instanceof payjoin.ProvisionalConfirmError.WrongBuffer,
        );
    });

    test("confirm twice returns AlreadyConsumed", () => {
        const { provisional, buf } = stageProvisional();
        const persister = new InMemoryReceiverPersister();
        persister.drain(buf);
        provisional.confirm(buf); // first call succeeds and consumes
        assert.throws(
            () => provisional.confirm(buf),
            (err: Error) =>
                err instanceof payjoin.ProvisionalConfirmError.AlreadyConsumed,
        );
    });
});
