import logging
import shutil
import os
import bloark


# Define a modifier profile.
class PTFModifier(bloark.ModifierProfile):
    count: int = 0

    def __init__(self):
        self.count = 0

    def block(self, content: dict, metadata: dict):
        # TODO: Modify the data and metadata here.
        #  If you want to skip this block, simply return `None, metadata`.
        #  If you want to skip the entire segment, simply return `None, None`.
        #  Check documentation of Modifier for more details.
        self.count += 1
        logging.debug(f'Modifier: test printout! {self.count}')
        return content, metadata


def test_minimal_modification_process():
    modifier = bloark.Modifier(output_dir='./tests/output', num_proc=8, log_level=logging.INFO)
    modifier.preload('./tests/sample_data/sample_warehouses')

    expected_files = map(lambda x: os.path.basename(x), modifier.files)

    modifier.add_profile(PTFModifier())
    modifier.start()

    assert os.path.exists('./tests/output')
    processed_files = os.listdir('./tests/output')
    assert set(processed_files) == set(expected_files)

    shutil.rmtree('./tests/output')
