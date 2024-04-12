from orc.context import Context
from tests.test_pipeline import init_zero


def test_context_initialization():
    ctx = Context()
    assert len(ctx.data) == 0
    assert ctx.output_queue.empty()


def test_add_context_key():
    ctx = Context()
    ctx.add_context_key('test_key', init_zero)
    assert not ctx.data

    ctx.init_data()
    assert 'test_key' in ctx.data
    assert ctx.data['test_key'] == 0
    assert 'test_key' in ctx.locks
