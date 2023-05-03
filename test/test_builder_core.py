import shutil

from utils import get_mock_7z_temporary_dir
import twb


def test_build_preload():
    # Test that nothing is raised.
    builder = twb.Builder(output_dir='./test/output')
    builder.preload('./test/sample_data/minimal_sample.xml')


def test_build_compressed():
    builder = twb.Builder(output_dir='./test/output', num_proc=4)
    builder.preload(get_mock_7z_temporary_dir())
    builder.build()
    shutil.rmtree('./test/output')
