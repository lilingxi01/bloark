import logging
import shutil
import os

from utils import get_mock_7z_temporary_dir
import twb


def test_build_preload():
    # Test that nothing is raised.
    builder = twb.Builder(output_dir='./test/output')
    builder.preload('./test/sample_data/minimal_sample.xml')


def test_build_compressed():
    builder = twb.Builder(output_dir='./test/output', num_proc=4, log_level=logging.DEBUG)
    builder.preload(get_mock_7z_temporary_dir())
    builder.build()

    # Make sure that the `./test/output/temp` folder is empty.
    assert not os.path.exists('./test/output/temp') or not os.listdir('./test/output/temp')

    shutil.rmtree('./test/output')
