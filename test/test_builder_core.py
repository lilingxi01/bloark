import os
import shutil

from utils.mock_7z_files import get_mock_multiple_article_7z_filename
from utils import test_file_count, get_mock_7z_temporary_dir
import twb


def test_build_preload():
    # Test that nothing is raised.
    builder = twb.Builder()
    builder.preload('./test/sample_data/minimal_sample.xml')


def test_build_no_compress():
    builder = twb.Builder(num_proc=4)
    builder.preload(get_mock_7z_temporary_dir())
    builder.build(output_dir='./test/output', compress=False)

    for i in range(test_file_count):
        assert os.path.exists(f'./test/output/block_{str(i).zfill(8)}.jsonl')

    shutil.rmtree('./test/output')


def test_build_compressed():
    builder = twb.Builder(num_proc=4)
    builder.preload(get_mock_7z_temporary_dir())
    builder.build(output_dir='./test/output')

    for i in range(test_file_count):
        assert os.path.exists(f'./test/output/block_{str(i).zfill(8)}.jsonl.zst')

    shutil.rmtree('./test/output')


def test_build_log_generation():
    builder = twb.Builder(log_dir='./test/logs', num_proc=4)
    builder.preload(get_mock_7z_temporary_dir())
    builder.build(output_dir='./test/output')

    for i in range(test_file_count):
        assert os.path.exists(f'./test/output/block_{str(i).zfill(8)}.jsonl.zst')

    assert os.path.exists('./test/logs/builder.log')

    shutil.rmtree('./test/output', ignore_errors=True)
    shutil.rmtree('./test/logs', ignore_errors=True)


def test_build_revision_splits():
    builder = twb.Builder(num_proc=1, revisions_per_block=1)
    builder.preload(get_mock_multiple_article_7z_filename())
    builder.build(output_dir='./test/output')

    assert len(os.listdir('./test/output')) == 7

    for i in range(len(os.listdir('./test/output'))):
        assert os.path.exists(f'./test/output/block_{str(i).zfill(8)}.jsonl.zst')

    shutil.rmtree('./test/output')
