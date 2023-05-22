import logging
import shutil
import os

from .utils import get_mock_7z_temporary_dir
import twb


def test_build_preload():
    # Test that nothing is raised.
    builder = twb.Builder(output_dir='./tests/output')
    builder.preload('./tests/sample_data/minimal_sample.xml')


def test_build_compressed():
    builder = twb.Builder(output_dir='./tests/output', num_proc=4, log_level=logging.DEBUG)
    builder.preload(get_mock_7z_temporary_dir())
    builder.build()

    # Make sure that the `./tests/output/temp` folder is empty.
    assert not os.path.exists('./tests/output/temp') or not os.listdir('./tests/output/temp')

    shutil.rmtree('./tests/output')
