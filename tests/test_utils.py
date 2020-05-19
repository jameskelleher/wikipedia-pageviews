from wiki_counts.utils import filename_from_path

def test_filename_from_path():
    path = '/i/am/a/path/file.gz'
    assert filename_from_path(path) == 'file.gz'

def test_filename_from_path_on_url():
    path = 'https://www.nicetesting.com/file.gz'
    assert filename_from_path(path) == 'file.gz'

def test_filename_from_path_removes_gz():
    path = '/another/nice/path/to/file.gz'
    assert filename_from_path(path, remove_gz=True) == 'file'