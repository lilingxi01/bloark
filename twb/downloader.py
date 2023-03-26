import logging
import os
from typing import List, Union
import requests
from bs4 import BeautifulSoup
from abc import ABC, abstractmethod

from twb.logger import universal_logger_init, twb_logger, cleanup_logger
from twb.parallelization import RDSProcessManager, RDSProcessController
from twb.utils import get_curr_version, prepare_output_dir

_DEFAULT_NUM_PROC = 1
_DEFAULT_LOG_LEVEL = logging.INFO


class Downloader:
    """
    A downloader for downloading files from the web.

    Attributes:
        profile: The download profile.
        url_batches: The batches of URLs to download.
        context: The context for the download profile.
        downloaded_files: The downloaded files.
        is_started: Whether the downloader is started.
        is_completed: Whether the downloader is completed.
    """
    def __init__(self,
                 log_dir: Union[str, None] = None,
                 num_proc: Union[int, None] = _DEFAULT_NUM_PROC,
                 log_level: int = _DEFAULT_LOG_LEVEL):
        """
        :param log_dir: The log directory. (default: None)
        :param num_proc: The number of processes to use for downloading. (default: number of CPUs)
                         (Might be limited by the profile.)
        :param log_level: The log level. (default: logging.DEBUG)
        """
        self.profile = None
        self.url_batches = []
        self.context = dict()
        self.downloaded_files = []
        self.is_started = False
        self.is_completed = False

        self.num_proc = num_proc

        cleanup_logger(log_name='downloader', log_dir=log_dir)

        universal_logger_init(log_name='downloader', log_dir=log_dir, log_level=log_level)

        self.log_dir = log_dir
        self.log_level = log_level

    def will_download_wiki_history_dump(self, date: Union[str, None] = None):
        """
        Returns True if the downloader will download the wiki history dump for the given date.
        :param date: The date of the wiki history dump. Format: YYYYMMDD. (default: latest)
        :raises Exception: If the downloader is already downloading a profile.
        :return: True if the downloader will download the wiki history dump for the given date.
        """
        if self.profile is not None:
            raise Exception("One downloader can only download one profile at a time.")

        # Assign the date to the context only if it is not None.
        if date is not None:
            self.context["date"] = date

        # Assign the profile.
        self.profile = WikiHistoryDumpDownloadProfile()

        url_batches = self.profile.get_url_batches(self.context)
        url_batches = list(set(url_batches))  # Remove duplicates.

        # Check the filename in the URL to avoid overwriting.
        filenames = set()
        for url in url_batches:
            curr_filename = url.split('/')[-1]
            if curr_filename in filenames:
                url_batches.remove(url)
            else:
                filenames.add(curr_filename)

        self.url_batches = url_batches

        twb_logger.info(f'Find {len(self.url_batches)} files downloadable in this profile.')

        if len(self.url_batches) == 0:
            raise Warning("No URLs are found for the given date.")

    def start(self, output_dir: str, limit: Union[int, None] = None):
        """
        Start the downloader.
        :param output_dir: The output directory.
        :param limit: The limit of the number of files to download. (default: None)
        """
        # Log the version.
        twb_logger.info(f'TWB Package Version: {get_curr_version()}')

        if self.profile is None:
            raise Exception("No profile is assigned to the downloader.")
        if self.url_batches is None or len(self.url_batches) == 0:
            raise Exception("No URLs are assigned to the downloader.")
        if self.is_completed:
            raise Exception("The downloader is already completed.")
        if self.is_started:
            raise Exception("The downloader is already started.")

        self.is_started = True

        url_batches = self.url_batches
        num_proc = self.num_proc

        # Check the filename locally to avoid overwriting.
        for url in url_batches:
            target_path = os.path.join(output_dir, url.split('/')[-1])
            if os.path.exists(target_path):
                url_batches.remove(url)

        # Limit the number of files to download.
        if limit is not None:
            if limit <= 0:
                twb_logger.critical('If you want to download all files, please set limit to None or do not define it.',
                                    '0 or less than 0 are not valid limit number.')
                return
            url_batches = url_batches[:limit]

        if len(url_batches) == 0:
            twb_logger.warning('No files need to be downloaded. Directly exit.')
            self.is_completed = True
            return

        # Prepare the output directory.
        prepare_output_dir(output_dir)

        # Log the number of files to download.
        twb_logger.info(f'Will download {len(url_batches)} files.')

        if self.profile.max_processes is not None:
            max_num_proc = self.profile.max_processes
            if num_proc is not None and num_proc > max_num_proc:
                twb_logger.warning(f'The number of processes is limited to {max_num_proc} by the profile.')
            num_proc = min(max_num_proc, num_proc if num_proc is not None else os.cpu_count())
        else:
            num_proc = num_proc if num_proc is not None else os.cpu_count()

        # Download the files.
        twb_logger.info(f'Start downloading. (# of assigned processes: {num_proc})')

        pm = RDSProcessManager(
            log_name='downloader',
            log_dir=self.log_dir,
            log_level=self.log_level,
            num_proc=num_proc
        )

        curr_finished = 0
        total_files = len(url_batches)
        downloaded_files = []

        def _success_callback(*args):
            nonlocal downloaded_files, curr_finished
            downloaded_files.append(args[0])
            curr_finished += 1
            curr_progress = curr_finished / total_files * 100
            twb_logger.info(f'Progress: {curr_finished} / {total_files} = ({curr_progress:.2f}%)')

        def _error_callback(e):
            nonlocal curr_finished
            curr_finished += 1
            curr_progress = curr_finished / total_files * 100
            twb_logger.error(f'Progress: {curr_finished} / {total_files} = ({curr_progress:.2f}%) (Terminated)')

            # Log the error.
            twb_logger.error(f'Error occurred when downloading: {e}')

        # Start processes.
        for url in url_batches:
            pm.apply_async(
                executable=download_executor,
                args=(url, output_dir),
                callback=_success_callback,
                error_callback=_error_callback,
                use_controller=True
            )

        # Wait for all processes to finish.
        pm.close()
        pm.join()

        twb_logger.info(f'Downloading completed. {len(downloaded_files)} files are downloaded.')

        self.downloaded_files = downloaded_files
        self.is_completed = True

        twb_logger.debug('Program ends.')


class DownloadProfile(ABC):
    max_processes: Union[int, None] = None
    """
    The maximum number of processes to use for downloading.
    """

    @abstractmethod
    def get_url_batches(self, context: dict) -> List[str]:
        """
        Returns a list of batches of URLs to download.
        :return: A list of batches of URLs to download.
        """
        pass


# Download profile for the wiki history dump.
class WikiHistoryDumpDownloadProfile(DownloadProfile):
    max_processes = 3

    root_url = 'https://dumps.wikimedia.org'
    master_url = 'https://dumps.wikimedia.org/enwiki/'

    # Update this variable whenever the latest dump data is changed. Check `master_url` for the latest date.
    # Pick the date that is the closest to the current date AND must have the 7z history dump (some latest dates
    # do not have it).
    latest_dump_date = '20230301'

    def get_url_batches(self, context: dict) -> List[str]:
        urls = []
        context_date = context['date'] if 'date' in context else self.latest_dump_date
        url = self.master_url + context_date + '/'
        page = requests.get(url)
        soup = BeautifulSoup(page.content, "html.parser")
        results = soup.find_all('a', href=True)
        for f in results:
            href_link = f['href']
            if f'enwiki-{context_date}-pages-meta-history' in href_link and '7z' in href_link and 'xml' in href_link:
                urls.append(self.root_url + href_link)
        return urls


# TODO: Progress bar for downloading.
def download_executor(controller: RDSProcessController, url: str, target_dir: str):
    # Compute the target path. Should assume that target dir exists.
    target_path = os.path.join(target_dir, url.split('/')[-1])
    # Download the file using a stream.
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(target_path, 'wb') as f:
            # Write the file in chunks of 10 MB for reduced memory usage.
            for chunk in r.iter_content(chunk_size=10 * 1024 * 1024):
                f.write(chunk)
    controller.logdebug(f'Downloaded {target_path}.')
    return target_path
