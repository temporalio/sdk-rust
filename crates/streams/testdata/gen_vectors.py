"""Writes the shared test vectors of the Stream runtime.

This is a second implementation of the layout, in Python, so the crate's tests check the Rust
code against it. The crate's README says which side reads each file. Run it from this directory:

    python3 gen_vectors.py
"""

import hashlib
import json
from urllib.parse import quote

SEQ_BITS = 20


def part(text):
    return quote(text, safe="")


def keys(prefix, stream_id):
    base = f"{part(prefix)}:{{{part(stream_id)}}}"
    return {
        "meta": f"{base}:meta",
        "log": f"{base}:log",
        "closed": f"{base}:closed",
    }


def be64(value):
    return max(value, 0).to_bytes(8, "big")


def be32(value):
    return max(value, 0).to_bytes(4, "big")


def task_key(started_event_id=0, attempt=0, started_time=(0, 0)):
    """24 bytes, which compare as bytes in the order of the fields."""
    return be64(started_event_id) + be32(attempt) + be64(started_time[0]) + be32(started_time[1])


def epoch_key(epoch):
    """40 bytes, the task key and then the scheduled time and the attempt."""
    scheduled = epoch.get("attempt_scheduled_time", (0, 0))
    task = task_key(
        epoch.get("task_started_event_id", 0),
        epoch.get("task_attempt", 0),
        tuple(epoch.get("task_started_time", (0, 0))),
    )
    return task + be64(scheduled[0]) + be32(scheduled[1]) + be32(epoch.get("attempt", 0))


def ids():
    """A Stream id is opaque, so the keys take its bytes, whatever they hold."""
    cases = []
    for prefix, stream_id in [
        ("temporal-streams", "agent-1:run-1:turn-3"),
        ("p:x", "a%3Ab%2Fc:r%251:%C3%A9%20name"),
        ("temporal-streams", "{x}:a|b é"),
    ]:
        cases.append({"prefix": prefix, "stream_id": stream_id, **keys(prefix, stream_id)})
    return {"streams": cases}


def offsets():
    cases = []
    for entry_id in ["0-1", "1-0", "1700000000000-0", "1700000000000-7", "1760000000123-1048575"]:
        ms, seq = (int(x) for x in entry_id.split("-"))
        cases.append({"entry_id": entry_id, "offset": (ms << SEQ_BITS) | seq})
    return cases


def epochs():
    cases = [
        {"kind": "workflow", "epoch": {"task_started_event_id": 8, "task_attempt": 2,
                                        "task_started_time": [1760000000, 5]}},
        {"kind": "local_activity", "epoch": {"task_started_event_id": 8, "task_attempt": 1,
                                              "task_started_time": [1760000000, 5],
                                              "attempt": 3}},
        {"kind": "activity", "epoch": {"attempt_scheduled_time": [1760000001, 999999999],
                                        "attempt": 2}},
        {"kind": "client", "epoch": {}},
        {"kind": "empty", "epoch": {}},
    ]
    for case in cases:
        case["key"] = epoch_key(case["epoch"]).hex()
        epoch = case["epoch"]
        case["task_key"] = task_key(
            epoch.get("task_started_event_id", 0),
            epoch.get("task_attempt", 0),
            tuple(epoch.get("task_started_time", (0, 0))),
        ).hex()
    return cases


def entry_id(text):
    """An entry id in a Producer field, the ms in 8 bytes and the seq in 4, zero for none."""
    if not text:
        return bytes(12)
    ms, seq = (int(x) for x in text.split("-"))
    return ms.to_bytes(8, "big") + seq.to_bytes(4, "big")


def producer_value(fence, sequence, first, last, digest):
    """The 88 bytes of a `p|` field."""
    return fence + sequence.to_bytes(8, "big") + entry_id(first) + entry_id(last) + digest


def producer_values():
    with_records = epoch_key({"attempt_scheduled_time": [1760000001, 999999999], "attempt": 2})
    empty = epoch_key({"attempt_scheduled_time": [1760000002, 0], "attempt": 3})
    cases = []
    for name, fence, sequence, first, last, records in [
        ("a batch with Records", with_records, 3, "1760000000123-4", "1760000000123-6",
         [{"data": "01"}]),
        ("an empty batch that wrote no entry", empty, 1, "", "", []),
    ]:
        batch_digest = bytes.fromhex(digest(records))
        value = producer_value(fence, sequence, first, last, batch_digest)
        cases.append({
            "name": name,
            "fence": fence.hex(),
            "sequence": sequence,
            "first": first,
            "last": last,
            "digest": batch_digest.hex(),
            "value": value.hex(),
        })
    return cases


def fields():
    """A Producer id is opaque, so its field name takes the bytes, also a separator in them."""
    producer = "ingest|eu:1 é"
    return {
        "kind": 3,
        "producer_id": producer,
        "producer": f"p|3|{producer}",
        "producer_values": producer_values(),
    }


def digest(records):
    """The content digest of a batch, which a dedupe entry keeps, as the first 16 bytes of the
    SHA-256."""
    out = hashlib.sha256()

    def part(data):
        out.update(len(data).to_bytes(8, "big"))
        out.update(data)

    for record in records:
        # A record with none of these keys has no body.
        if not {"metadata", "data", "external_payloads"} & record.keys():
            out.update(b"\x00")
            continue
        out.update(b"\x01")
        metadata = record.get("metadata", {})
        metadata = sorted((key.encode(), bytes.fromhex(value)) for key, value in metadata.items())
        out.update(len(metadata).to_bytes(8, "big"))
        for key, value in metadata:
            part(key)
            part(value)
        part(bytes.fromhex(record.get("data", "")))
        external = record.get("external_payloads", [])
        out.update(len(external).to_bytes(8, "big"))
        for size in external:
            out.update(size.to_bytes(8, "big", signed=True))
    return out.digest()[:16].hex()


def digests():
    """Batches as hex bytes, with the hex digest that the crate's `batch_digest` must compute."""
    cases = [
        {"name": "empty batch", "records": []},
        {"name": "one record, no body", "records": [{}]},
        {"name": "one record, an empty body", "records": [{"data": ""}]},
        {"name": "one record", "records": [{"data": b"hello".hex()}]},
        {
            "name": "metadata in key order",
            "records": [
                {
                    "metadata": {"encoding": b"json/plain".hex(), "a": "00ff"},
                    "data": b'{"x":1}'.hex(),
                }
            ],
        },
        {"name": "two records", "records": [{"data": "01"}, {"data": "0203"}]},
        {"name": "external payloads", "records": [{"data": "01", "external_payloads": [10, 20]}]},
        # The length prefixes give these two different digests, though their joined bytes are equal.
        {
            "name": "metadata key and value split one way",
            "records": [{"metadata": {"ab": "63"}}],
        },
        {
            "name": "metadata key and value split another way",
            "records": [{"metadata": {"a": "6263"}}],
        },
    ]
    for case in cases:
        case["digest"] = digest(case["records"])
    assert cases[-1]["digest"] != cases[-2]["digest"]
    assert cases[1]["digest"] != cases[2]["digest"]
    return cases


def main():
    for name, value in [
        ("ids.json", ids()),
        ("offsets.json", offsets()),
        ("epochs.json", epochs()),
        ("fields.json", fields()),
        ("digests.json", digests()),
    ]:
        with open(name, "w") as out:
            json.dump(value, out, indent=2, ensure_ascii=False)
            out.write("\n")


if __name__ == "__main__":
    main()
