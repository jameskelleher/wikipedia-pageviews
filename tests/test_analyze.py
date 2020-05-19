from wiki_counts.analyze import (
    in_blacklist_set,
    add_to_blacklist,
    add_to_heap_map,
    get_line_info
)

import pytest
import heapq

from collections import defaultdict


@pytest.fixture
def domain_code():
    return 'domain_code'


@pytest.fixture
def page_title():
    return 'page_title'


@pytest.fixture
def count_views():
    return 5


@pytest.fixture
def heap_map_size_3():
    key = 'domain_code'
    heap_map = {key: []}
    heapq.heappush(heap_map[key], (3, 'page1'))
    heapq.heappush(heap_map[key], (4, 'page2'))
    heapq.heappush(heap_map[key], (5, 'page3'))

    return heap_map


def test_in_blacklist_set():
    test_set = set()
    test_set.add(('en', 'my_page'))
    assert in_blacklist_set('en', 'my_page', test_set)


def test_not_in_blacklist_set():
    test_set = set()
    test_set.add(('en', 'not_my_page'))
    assert not in_blacklist_set('en', 'my_page', test_set)


def test_add_to_blacklist_good_data():
    test_set = set()
    line = 'domain_code page_name'
    add_to_blacklist(line, test_set)
    assert ('domain_code', 'page_name') in test_set


def test_add_to_blacklist_extra_spaces():
    test_set = set()
    line = 'domain_code    \t    page_name'
    add_to_blacklist(line, test_set)
    assert ('domain_code', 'page_name') in test_set


def test_add_to_blacklist_has_newline():
    test_set = set()
    line = 'domain_code page_name\n'
    add_to_blacklist(line, test_set)
    assert ('domain_code', 'page_name') in test_set


def test_too_much_data_into_blacklist_raises_exception():
    test_set = set()
    line = 'domain_code page_name and some junk too'
    with pytest.raises(AssertionError):
        add_to_blacklist(line, test_set)


def test_too_litte_data_into_blacklist_raises_exception():
    test_set = set()
    line = 'hi'
    with pytest.raises(AssertionError):
        add_to_blacklist(line, test_set)


def test_add_to_heap_map_empty(domain_code, page_title, count_views):
    test_heap_map = {'domain_code': []}
    add_to_heap_map(
        most_viewed_map=test_heap_map,
        domain_code=domain_code,
        page_title=page_title,
        count_views=count_views,
        top_n_pageviews=25)

    assert test_heap_map[domain_code] == [(count_views, page_title)]


def test_add_to_heap_defaultdict_empty(domain_code, page_title, count_views):
    test_heap_map = defaultdict(list)

    add_to_heap_map(
        most_viewed_map=test_heap_map,
        domain_code=domain_code,
        page_title=page_title,
        count_views=count_views,
        top_n_pageviews=25)

    assert test_heap_map[domain_code] == [(count_views, page_title)]


def test_add_to_heap_still_room_left(domain_code, page_title, heap_map_size_3):
    my_count_views = 2

    add_to_heap_map(
        most_viewed_map=heap_map_size_3,
        domain_code=domain_code,
        page_title=page_title,
        count_views=my_count_views,
        top_n_pageviews=25)

    assert (2, page_title) in heap_map_size_3[domain_code]


def test_dont_add_to_heap_if_smallest_and_full(domain_code, page_title, heap_map_size_3):
    my_count_views = 2

    add_to_heap_map(
        most_viewed_map=heap_map_size_3,
        domain_code=domain_code,
        page_title=page_title,
        count_views=my_count_views,
        top_n_pageviews=3)

    assert (my_count_views, page_title) not in heap_map_size_3[domain_code]


def test_heap_adds_bigger_element(domain_code, page_title, heap_map_size_3):
    my_count_views = 4

    add_to_heap_map(
        most_viewed_map=heap_map_size_3,
        domain_code=domain_code,
        page_title=page_title,
        count_views=my_count_views,
        top_n_pageviews=3)

    assert (my_count_views, page_title) in heap_map_size_3[domain_code]


def test_heap_removes_smallest_element(domain_code, page_title, heap_map_size_3):
    my_count_views = 4

    add_to_heap_map(
        most_viewed_map=heap_map_size_3,
        domain_code=domain_code,
        page_title=page_title,
        count_views=my_count_views,
        top_n_pageviews=3)

    assert (3, page_title) not in heap_map_size_3[domain_code]


def test_get_line_info(domain_code, page_title, count_views):
    line = f'{domain_code} {page_title} {count_views} 0'
    assert (domain_code, page_title, count_views) == get_line_info(line)


def test_get_line_info_extra_whitespace(domain_code, page_title, count_views):
    line = f'{domain_code}    {page_title} \t\t {count_views} 0'
    assert (domain_code, page_title, count_views) == get_line_info(line)


def test_get_line_info_with_newline(domain_code, page_title, count_views):
    line = f'{domain_code} {page_title} {count_views} 0\n'
    assert (domain_code, page_title, count_views) == get_line_info(line)


def test_get_line_info_raises_on_too_much():
    line = 'i am some junk data i am really bad'
    with pytest.raises(AssertionError):
        get_line_info(line)


def test_get_line_info_raises_on_too_little():
    line = 'hi'
    with pytest.raises(AssertionError):
        get_line_info(line)


def test_get_line_info_raises_on_failed_int_cast():
    line = 'good good i_should_be_int unimportant'
    with pytest.raises(ValueError):
        get_line_info(line)
