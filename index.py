
import re
import m3u8
import asyncio
import aiohttp
import aiofiles

from pathlib import Path
from datetime import datetime
from tqdm.asyncio import tqdm


class M3U8Downloader:

    def __init__(self, url, base_dir: Path, filename: str = 'final'):
        self.base_dir = base_dir
        self.url = url

        self.folder = self.base_dir / f'{filename}'
        self.folder.mkdir(exist_ok=True)

        self.segments_dir = self.folder / 'segments'
        self.segments_dir.mkdir(exist_ok=True)

        self.final_file = self.folder / f'{filename}.ts'

        self.segments_queue = asyncio.Queue()

        self.allowed_extensions = ('.ts',)

    @staticmethod
    def generate_unique_filename(file_path: Path):
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        file_path = file_path.with_stem(f'{file_path.stem}_{timestamp}')
        return file_path

    @staticmethod
    def natural_sort_key(char: str):
        return [int(text) if text.isdigit() else text.lower() for text in re.split(r'(\d+)', char)]

    async def __generate_segments(self):
        playlist = m3u8.load(self.url)
        mylist = [x for x in playlist.data['segments']]
        for index, x in enumerate(mylist):
            segment = {**x, 'id': index}
            await self.segments_queue.put(segment)

    async def __download_segment(self, session: aiohttp.ClientSession, task, retries=5):
        task_id, url = task['id'], task['uri']
        filename = self.segments_dir / f'segment{task_id:04d}.ts'
        for attempt in range(retries):
            try:
                async with session.get(url) as response:
                    response.raise_for_status()
                    pbar = tqdm(desc=filename.stem, total=9e9, unit='B',
                                unit_scale=True, dynamic_ncols=True, ascii=True, colour="#ff0000")
                    async with aiofiles.open(filename, 'wb') as f:
                        async for data in response.content.iter_any():
                            await f.write(data)
                            pbar.update(len(data))
                    pbar.close()
                break  # Success, no need to retry
            except aiohttp.ClientPayloadError as e:
                print(f"Payload error on attempt {attempt + 1} for {url}: {e}")
            except aiohttp.ClientError as e:
                print(f"Client error on attempt {attempt + 1} for {url}: {e}")
            except Exception as e:
                print(
                    f"Unexpected error on attempt {attempt + 1} for {url}: {e}")
            await asyncio.sleep(2)  # Wait before retrying

    async def __worker(self, session: aiohttp.ClientSession, semaphore: asyncio.Semaphore):
        while not self.segments_queue.empty():
            task = await self.segments_queue.get()
            async with semaphore:  # Ensure concurrency limit
                await self.__download_segment(session, task)
            self.segments_queue.task_done()

    async def __download_segments(self):
        await self.__generate_segments()
        semaphore = asyncio.Semaphore(10)  # Limit to 10 concurrent workers
        async with aiohttp.ClientSession() as session:
            # Create multiple workers to download segments concurrently
            tasks = [self.__worker(session, semaphore) for _ in range(10)]
            await asyncio.gather(*tasks)

    async def __merge_segments(self):
        segment_files = sorted(self.segments_dir.glob(
            '*.ts'), key=lambda f: self.natural_sort_key(f.name))
        total_size = sum(file.stat().st_size for file in segment_files)
        self.final_file = self.generate_unique_filename(self.final_file)
        pbar = tqdm(total=total_size, unit='B', unit_scale=True,
                    desc="Merging Segments",  ascii=True, colour="#ff0000")
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

    async def __clean_up(self):
        for f in self.segments_dir.iterdir():
            await asyncio.to_thread(f.unlink)
        # await asyncio.to_thread(self.segments_dir.rmdir)

    async def run(self):
        try:
            await self.__download_segments()
            await self.__merge_segments()
        finally:
            await self.__clean_up()


if __name__ == '__main__':
    folder = Path(__file__).resolve().parent
    url = input("Enter the m3u8 url: ")

    downloader = M3U8Downloader(url, folder)
    asyncio.run(downloader.run())
