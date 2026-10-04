"""WeightManager / StickyAssigner unit behaviour."""
from __future__ import annotations

import threading
from collections import Counter

import pytest

from gateway.weights import StickyAssigner, WeightManager

# -- WeightManager -------------------------------------------------------

def test_unconfigured_route_reports_none():
    wm = WeightManager()
    assert wm.weights_for("r") is None
    selected, info = wm.pick("r", "client-1", ["a", "b"])
    assert selected is None and info["reason"] == "weights_not_configured"


def test_pick_is_deterministic_and_explainable():
    wm = WeightManager({"r": {"a": 1, "b": 3}})
    first, info1 = wm.pick("r", "client-42", ["a", "b"])
    for _ in range(5):
        again, _ = wm.pick("r", "client-42", ["a", "b"])
        assert again == first  # reproducible for the same key
    # explanation lets an operator recompute the decision by hand
    assert info1["bucket"] == info1["hash"] % 4
    assert info1["total_weight"] == 4
    lo, hi = info1["ranges"][first]
    assert lo <= info1["bucket"] < hi
    assert info1["ranges"] == {"a": [0, 1], "b": [1, 4]}


def test_distribution_follows_weights():
    wm = WeightManager({"r": {"a": 1, "b": 3}})
    counts = Counter()
    for i in range(4000):
        uid, _ = wm.pick("r", f"client-{i}", ["a", "b"])
        counts[uid] += 1
    ratio = counts["b"] / (counts["a"] + counts["b"])
    assert 0.70 < ratio < 0.80, f"expected ~0.75 for b, got {ratio}"


def test_weight_zero_gets_no_traffic_and_missing_means_zero():
    wm = WeightManager({"r": {"a": 1, "b": 0}})  # 'c' absent -> implicit 0
    for i in range(200):
        uid, info = wm.pick("r", f"k{i}", ["a", "b", "c"])
        assert uid == "a"
    assert set(info["weight_zero_skipped"]) == {"b", "c"}


def test_all_zero_weights_selects_nothing():
    wm = WeightManager({"r": {"a": 0, "b": 0}})
    selected, info = wm.pick("r", "k", ["a", "b"])
    assert selected is None and info["reason"] == "all_weights_zero"


def test_set_weights_at_runtime_changes_new_picks():
    wm = WeightManager({"r": {"a": 1, "b": 0}})
    before, _ = wm.pick("r", "client-9", ["a", "b"])
    assert before == "a"
    change = wm.set_weights("r", {"a": 0, "b": 1})
    assert change["previous"] == {"a": 1, "b": 0}
    assert change["version"] == 2
    after, info = wm.pick("r", "client-9", ["a", "b"])
    assert after == "b" and info["version"] == 2


def test_invalid_weights_rejected():
    wm = WeightManager()
    for bad in ({"a": -1}, {"a": 1.5}, {"a": True}):
        with pytest.raises(ValueError):
            wm.set_weights("r", bad)


def test_snapshot_lists_routes():
    wm = WeightManager({"r": {"a": 2, "b": 1}})
    snap = wm.snapshot()
    assert snap["r"]["weights"] == {"a": 2, "b": 1}
    assert snap["r"]["total_weight"] == 3


# -- StickyAssigner ------------------------------------------------------

class FakeClock:
    def __init__(self, start=0.0):
        self.t = start

    def __call__(self):
        return self.t

    def advance(self, dt):
        self.t += dt


def _chooser(seq):
    it = iter(seq)

    def choose():
        return next(it), {"policy": "weighted"}

    return choose


def test_sticky_hit_reuses_assignment_and_slides_ttl():
    clock = FakeClock()
    sa = StickyAssigner(clock=clock)
    always_valid = lambda u: True
    uid, info = sa.lookup_or_assign("r", "c1", 10.0, always_valid, _chooser(["a", "b"]))
    assert uid == "a" and info["sticky"] == "new"
    clock.advance(6.0)
    uid, info = sa.lookup_or_assign("r", "c1", 10.0, always_valid, _chooser(["b"]))
    assert uid == "a" and info["sticky"] == "hit"  # chooser not consulted
    clock.advance(9.0)  # within the slid window (expires at 16)
    uid, _ = sa.lookup_or_assign("r", "c1", 10.0, always_valid, _chooser(["b"]))
    assert uid == "a"


def test_sticky_expiry_reassigns_deterministically():
    clock = FakeClock()
    sa = StickyAssigner(clock=clock)
    always_valid = lambda u: True
    sa.lookup_or_assign("r", "c1", 5.0, always_valid, _chooser(["a"]))
    clock.advance(6.0)  # expired
    uid, info = sa.lookup_or_assign("r", "c1", 5.0, always_valid, _chooser(["b"]))
    assert uid == "b" and info["sticky"] == "expired"


def test_ineligible_target_triggers_reassignment():
    clock = FakeClock()
    sa = StickyAssigner(clock=clock)
    sa.lookup_or_assign("r", "c1", 100.0, lambda u: True, _chooser(["a"]))
    # 'a' got ejected / drained -> no longer valid
    uid, info = sa.lookup_or_assign("r", "c1", 100.0, lambda u: u != "a", _chooser(["b"]))
    assert uid == "b" and info["sticky"] == "target_ineligible"


def test_recovered_target_does_not_steal_client_back():
    clock = FakeClock()
    sa = StickyAssigner(clock=clock)
    eligible = {"a"}
    sa.lookup_or_assign("r", "c1", 100.0, lambda u: u in eligible, _chooser(["a"]))
    eligible.discard("a")  # a ejected
    eligible.add("b")
    uid, _ = sa.lookup_or_assign("r", "c1", 100.0, lambda u: u in eligible, _chooser(["b"]))
    assert uid == "b"
    eligible.add("a")  # a recovers before the assignment expires
    uid, info = sa.lookup_or_assign("r", "c1", 100.0, lambda u: u in eligible, _chooser(["a"]))
    assert uid == "b" and info["sticky"] == "hit"  # no flip back


def test_concurrent_requests_of_one_client_get_one_instance():
    sa = StickyAssigner()
    chosen: list[str] = []
    lock = threading.Lock()

    def choose():
        with lock:
            uid = f"u{len(chosen)}"
            chosen.append(uid)
            return uid, {}

    results: list[str | None] = []

    def worker():
        uid, _ = sa.lookup_or_assign("r", "hot-client", 60.0, lambda u: True, choose)
        results.append(uid)

    threads = [threading.Thread(target=worker) for _ in range(32)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(set(results)) == 1, f"client split across instances: {set(results)}"
    assert len(chosen) == 1  # choose ran exactly once


def test_failed_choose_is_not_memoised():
    sa = StickyAssigner()
    calls = iter([(None, {"reason": "all_weights_zero"}), ("a", {})])

    def choose():
        return next(calls)

    uid, _ = sa.lookup_or_assign("r", "c1", 10.0, lambda u: True, choose)
    assert uid is None
    uid, _ = sa.lookup_or_assign("r", "c1", 10.0, lambda u: True, choose)
    assert uid == "a"  # retried after the failed attempt


def test_sticky_snapshot_shows_entries():
    sa = StickyAssigner()
    sa.lookup_or_assign("r", "c1", 10.0, lambda u: True, _chooser(["a"]))
    snap = sa.snapshot()
    assert snap["r:c1"]["upstream"] == "a"
    assert snap["r:c1"]["expires_in"] > 0
