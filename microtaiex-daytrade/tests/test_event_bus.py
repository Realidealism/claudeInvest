import threading

from core.event_bus import EventBus


def test_single_thread_order_preserved():
    bus = EventBus()
    bus.start()
    got = []
    bus.subscribe("e", got.append)
    for i in range(100):
        bus.publish("e", i)
    bus.wait_idle()
    bus.stop()
    assert got == list(range(100))


def test_multi_thread_all_delivered_once():
    bus = EventBus()
    bus.start()
    got = []
    bus.subscribe("e", got.append)

    def worker(base):
        for i in range(100):
            bus.publish("e", base + i)

    threads = [threading.Thread(target=worker, args=(b * 1000,)) for b in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    bus.wait_idle()
    bus.stop()
    assert len(got) == 500
    assert len(set(got)) == 500


def test_handler_exception_isolated():
    bus = EventBus()
    bus.start()
    got = []

    def boom(_):
        raise ValueError("bad handler")

    bus.subscribe("e", boom)
    bus.subscribe("e", got.append)
    bus.publish("e", 1)
    bus.wait_idle()
    bus.stop()
    assert got == [1]
