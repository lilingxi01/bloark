import os
import shutil
import twb


download_output_dir = "./test/download_temp"


def test_wiki_history_dump_download():
    # Create a downloader.
    downloader = twb.Downloader()

    # Download the wiki history dump.
    downloader.will_download_wiki_history_dump()

    # Check the downloader status.
    assert len(downloader.url_batches) > 0

    # Compute the expected filenames.
    pending_filenames = list(map(lambda url: url.split('/')[-1], downloader.url_batches))

    try:
        # Start the downloader.
        downloader.start(output_dir=download_output_dir, num_proc=3, limit=4)

        assert downloader.is_started
        assert downloader.is_completed

        # Check the downloader status.
        assert len(downloader.downloaded_files) == 4

        # Check the downloaded files.
        for filepath in downloader.downloaded_files:
            filename = filepath.split('/')[-1]
            assert filename in pending_filenames
            assert os.path.exists(filepath)
    finally:
        # Clean up.
        shutil.rmtree(download_output_dir)
