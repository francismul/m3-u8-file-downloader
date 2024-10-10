
import logging
import re
import m3u8

import asyncio
import aiohttp
import aiofiles

from pathlib import Path
from datetime import datetime
from tqdm.asyncio import tqdm


logging.basicConfig(level=logging.DEBUG)


class M3U8Downloader:
    """
    Class to download and merge segments from an M3U8 playlist.

    Attributes:
        url (str): The URL of the M3U8 playlist.
        base_dir (Path): The base directory for storing downloaded files.
        folder (Path): Directory where all the files, including segments and the final merged file, will be saved.
        segments_dir (Path): Directory for storing individual video segments.
        final_file (Path): The path for the final merged video file.
        segments_queue (asyncio.Queue): A queue to manage segment download tasks.
        allowed_extensions (tuple): Allowed file extensions for segments.
    """

    def __init__(self, url: str, base_dir: Path, filename: str = 'final'):
        """
        Initializes the M3U8Downloader object.

        Args:
            url (str): The M3U8 playlist URL.
            base_dir (Path): Base directory to store the downloaded files.
            filename (str, optional): Name for the final merged file. Defaults to 'final'.
        """
        self.base_dir = base_dir
        self.url = url
        self.folder = self.base_dir / filename
        self.folder.mkdir(exist_ok=True)
        self.segments_dir = self.folder / 'segments'
        self.segments_dir.mkdir(exist_ok=True)
        self.final_file = self.folder / f'{filename}.ts'
        self.segments_queue = asyncio.Queue()
        self.allowed_extensions = ('.ts',)

    @staticmethod
    def generate_unique_filename(file_path: Path) -> Path:
        """
        Generates a unique filename by appending a timestamp to avoid overwriting.

        Args:
            file_path (Path): Original file path.

        Returns:
            Path: New file path with a timestamp appended.
        """
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        return file_path.with_stem(f'{file_path.stem}_{timestamp}')

    @staticmethod
    def natural_sort_key(char: str) -> list:
        """
        Generates a key for sorting filenames naturally (e.g., 1, 2, 10 instead of 1, 10, 2).

        Args:
            char (str): The string to be sorted.

        Returns:
            list: A list of strings and integers for natural sorting.
        """
        return [int(text) if text.isdigit() else text.lower() for text in re.split(r'(\d+)', char)]

    async def __generate_segments(self) -> None:
        """
        Generates a list of segments from the M3U8 playlist and adds them to the segments queue.
        """
        playlist = m3u8.load(self.url)
        for index, segment in enumerate(playlist.data['segments']):
            segment['id'] = index
            await self.segments_queue.put(segment)

    async def __download_segment(self, session: aiohttp.ClientSession, task: dict, retries: int = 5) -> None:
        """
        Downloads a single segment with retries upon failure.

        Args:
            session (aiohttp.ClientSession): The active aiohttp session for making requests.
            task (dict): A dictionary containing the segment id and URL.
            retries (int, optional): Number of retry attempts for failed downloads. Defaults to 5.
        """
        task_id, url = task['id'], task['uri']
        filename = self.segments_dir / f'segment{task_id:04d}.ts'
        pbar = tqdm(desc=filename.stem, total=None, unit='B',
                    unit_scale=True, dynamic_ncols=True, ascii=True, colour="#ff0000")

        for attempt in range(retries):
            try:
                async with session.get(url) as response:
                    response.raise_for_status()
                    async with aiofiles.open(filename, 'wb') as f:
                        async for data in response.content.iter_any():
                            await f.write(data)
                            pbar.update(len(data))
                break  # Success, no need to retry
            except aiohttp.ClientError as e:
                logging.error(f"Error on attempt {attempt + 1} for {url}: {e}")
                await asyncio.sleep(2)  # Wait before retrying
        pbar.close()

    async def __worker(self, session: aiohttp.ClientSession, semaphore: asyncio.Semaphore) -> None:
        """
        Worker coroutine to download segments concurrently.

        Args:
            session (aiohttp.ClientSession): The active aiohttp session.
            semaphore (asyncio.Semaphore): Semaphore to limit concurrent workers.
        """
        while not self.segments_queue.empty():
            task = await self.segments_queue.get()
            async with semaphore:  # Ensure concurrency limit
                await self.__download_segment(session, task)
            self.segments_queue.task_done()

    async def __download_segments(self) -> None:
        """
        Manages the downloading of all segments from the M3U8 playlist.
        """
        await self.__generate_segments()
        semaphore = asyncio.Semaphore(10)  # Limit to 10 concurrent workers
        async with aiohttp.ClientSession() as session:
            tasks = [self.__worker(session, semaphore) for _ in range(10)]
            await asyncio.gather(*tasks)

    async def __merge_segments(self) -> None:
        """
        Merges all downloaded segments into a single file.
        """
        segment_files = sorted(self.segments_dir.glob(
            '*.ts'), key=self.natural_sort_key)
        total_size = sum(file.stat().st_size for file in segment_files)
        self.final_file = self.generate_unique_filename(self.final_file)
        pbar = tqdm(total=total_size, unit='B', unit_scale=True,
                    desc="Merging Segments", ascii=True, colour="#ff0000")

        async with aiofiles.open(self.final_file, 'wb') as f:
            for file in segment_files:
                async with aiofiles.open(file, 'rb') as segment:
                    while True:
                        chunk = await segment.read()
                        if not chunk:
                            break
                        await f.write(chunk)
                        pbar.update(len(chunk))
        pbar.close()

    async def __clean_up(self) -> None:
        """
        Cleans up the segments directory by deleting the segment files and removing the directory.
        """
        for f in self.segments_dir.iterdir():
            await asyncio.to_thread(f.unlink)
        # Remove the segments directory
        await asyncio.to_thread(self.segments_dir.rmdir)

    async def run(self) -> None:
        """
        Main method to download and merge the segments, and then clean up temporary files.
        """
        try:
            await self.__download_segments()
            await self.__merge_segments()
        finally:
            await self.__clean_up()


if __name__ == '__main__':
    folder = Path(__file__).resolve().parent
    try:
        url = input("Enter the m3u8 url: ")
        downloader = M3U8Downloader(url, folder)
        asyncio.run(downloader.run())
    except KeyboardInterrupt:
        logging.info("Exiting... Download interrupted by user.")
