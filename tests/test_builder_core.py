import logging
import shutil
import os

from .utils import get_mock_7z_temporary_dir
import bloark


def test_build_preload():
    # Test that nothing is raised.
    builder = bloark.Builder(output_dir='./tests/output')
    builder.preload('./tests/sample_data/minimal_sample.xml')


def test_build_compressed_7z():
    builder = bloark.Builder(output_dir='./tests/output', num_proc=4, log_level=logging.DEBUG)
    builder.preload(get_mock_7z_temporary_dir())
    builder.build()

    # Make sure that the `./tests/output/temp` folder is empty.
    assert not os.path.exists('./tests/output/temp') or not os.listdir('./tests/output/temp')

    shutil.rmtree('./tests/output')


def test_build_compressed_bz2():
    builder = bloark.Builder(output_dir='./tests/output', num_proc=4, log_level=logging.DEBUG)
    builder.preload('./tests/sample_data/sample.bz2')
    builder.build()

    # Make sure that the `./tests/output/temp` folder is empty.
    assert not os.path.exists('./tests/output/temp') or not os.listdir('./tests/output/temp')

    shutil.rmtree('./tests/output')
