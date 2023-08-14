import logging
import bloark


# Define a modifier profile.
class PTFModifier(bloark.ModifierProfile):
    count: int = 0

    def __init__(self):
        self.count = 0

    def block(self, content: dict, metadata: dict):
        self.count += 1
        logging.debug(f'Modifier: test printout! {self.count}')
        return content, metadata


if __name__ == '__main__':
    # Create a modifier instance with 8 processes (CPUs) and INFO-level logging.
    modifier = bloark.Modifier(output_dir='./tests/output', num_proc=2, log_level=logging.INFO)

    # Preload all files from the input directory (original warehouses).
    modifier.preload('./tests/sample_data/sample_warehouses')

    # Add the modifier profile to the modifier instance.
    modifier.add_profile(PTFModifier())

    # Start modifying the warehouses (this command will take a long time).
    modifier.start()
