import os
import shutil

from utils import test_file_count, get_mock_7z_temporary_dir
import twb


def test_build_preload():
    # Test that nothing is raised.
    builder = twb.Builder()
    builder.preload('./test/sample_data/minimal_sample.xml')


def test_build_no_compress():
    builder = twb.Builder()
    builder.preload(get_mock_7z_temporary_dir())
    builder.build('./test/output', num_proc=4, compress=False)

    for i in range(test_file_count):
        assert os.path.exists(f'./test/output/block_{str(i).zfill(5)}.jsonl')

    shutil.rmtree('./test/output')


def test_build_compressed():
    builder = twb.Builder()
    builder.preload(get_mock_7z_temporary_dir())
    builder.build('./test/output', num_proc=4, compress=True)

    for i in range(test_file_count):
        assert os.path.exists(f'./test/output/block_{str(i).zfill(5)}.jsonl.zst')

    shutil.rmtree('./test/output')
