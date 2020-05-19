# contents of test_app.py, a simple test for our API retrieval
# import requests for the purposes of monkeypatching
from wiki_counts.download import handle_error, kill_process

import pytest
import asyncio


class MockRequestInfo:
    url = "i'm a url ;)"


class MockException:
    def __init__(self, status):
        self.status = status
        self.request_info = MockRequestInfo()


@pytest.fixture
def queue():
    return asyncio.Queue()


@pytest.mark.asyncio
async def test_handle_error_puts_503_back_in_queue(monkeypatch, queue):
    e = MockException(503)
    url = 'hi'

    async def mock_sleep(*args):
        pass

    monkeypatch.setattr(asyncio, 'sleep', mock_sleep)

    await handle_error(e, queue, url)
    result = await queue.get()

    assert result == url


@pytest.mark.asyncio
async def test_handle_error_does_not_put_404_back_in_queue(monkeypatch, queue):
    e = MockException(404)
    url = 'hi'
    await handle_error(e, queue, url)

    assert queue.empty()


@pytest.mark.asyncio
async def test_kill_process_empties_queue(queue):
    [queue.put_nowait(i) for i in range(10)]
    await kill_process(queue)
    
    assert queue.empty()
