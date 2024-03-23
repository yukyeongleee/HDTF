"""
This file downloads almost all the videos from the HDTF dataset. Some videos are discarded for the following reasons:
- they do not contain cropping information because they are somewhat noisy (hand moving, background changing, etc.)
- they are not available on youtube anymore (at all or in the specified format)

The discarded videos constitute a small portion of the dataset, so you can try to re-download them manually on your own.

Usage:
```
$ python download.py --output_dir /tmp/data/hdtf --num_workers 8
```
youtube-dl doesn't work anymore.
It use yt-dlp now instead of youtube-dl.
You need tqdm and yt-dlp libraries to be installed for this script to work.

"""


import os
import argparse
from typing import List, Dict
from multiprocessing import Pool
import subprocess
from subprocess import Popen, PIPE
from urllib import parse
from tqdm import tqdm

subsets = ["RD", "WDA", "WRA"]

def download_hdtf(source_dir: os.PathLike, output_dir: os.PathLike, num_workers: int):
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(os.path.join(output_dir, '_videos_raw'), exist_ok=True)

    download_queue = construct_download_queue(source_dir, output_dir)
    task_kwargs = [dict(video_data=vd, output_dir=output_dir) for vd in download_queue]
    pool = Pool(processes=num_workers)
    tqdm_kwargs = dict(total=len(task_kwargs), desc=f'Downloading videos into {output_dir}')

    for _ in tqdm(pool.imap_unordered(task_proxy, task_kwargs), **tqdm_kwargs):
        pass

    print('Download is finished, you can now (optionally) delete the following directories, since they are not needed anymore and occupy a lot of space:')
    print(' -', os.path.join(output_dir, '_videos_raw'))

def construct_download_queue(source_dir: os.PathLike, output_dir: os.PathLike) -> List[Dict]:
    download_queue = []

    for subset in subsets:
        video_urls = read_file_as_space_separated_data(os.path.join(source_dir, f'{subset}_video_url.txt'))
        crops = read_file_as_space_separated_data(os.path.join(source_dir, f'{subset}_crop_wh.txt'))
        intervals = read_file_as_space_separated_data(os.path.join(source_dir, f'{subset}_annotion_time.txt'))
        resolutions = read_file_as_space_separated_data(os.path.join(source_dir, f'{subset}_resolution.txt'))

        for video_name, (video_url,) in video_urls.items():
            if not f'{video_name}.mp4' in intervals:
                continue

            if not f'{video_name}.mp4' in resolutions or len(resolutions[f'{video_name}.mp4']) > 1:
                continue

            all_clips_intervals = [x.split('-') for x in intervals[f'{video_name}.mp4']]
            clips_crops = []
            clips_intervals = []

            for clip_idx, clip_interval in enumerate(all_clips_intervals):
                clip_name = f'{video_name}_{clip_idx}.mp4'
                if not clip_name in crops:
                    continue
                clips_crops.append(crops[clip_name])
                clips_intervals.append(clip_interval)

            clips_crops = [list(map(int, cs)) for cs in clips_crops]

            if len(clips_crops) == 0:
                continue

            download_queue.append({
                'name': f'{subset}_{video_name}',
                'id': parse.parse_qs(parse.urlparse(video_url).query)['v'][0],
                'intervals': clips_intervals,
                'crops': clips_crops,
                'output_dir': output_dir,
                'resolution': resolutions[f'{video_name}.mp4'][0]
            })

    return download_queue

def task_proxy(kwargs):
    return download_and_process_video(**kwargs)

def download_and_process_video(video_data: Dict, output_dir: str):
    raw_download_path = os.path.join(output_dir, '_videos_raw', f"{video_data['name']}.mp4")
    download_result = download_video(video_data['id'], raw_download_path, resolution=video_data['resolution'])

    if not download_result:
        return

    for clip_idx in range(len(video_data['intervals'])):
        start, end = video_data['intervals'][clip_idx]
        clip_name = f'{video_data["name"]}_{clip_idx:03d}'
        clip_path = os.path.join(output_dir, clip_name + '.mp4')
        crop_success = cut_and_crop_video(raw_download_path, clip_path, start, end, video_data['crops'][clip_idx])

        if not crop_success:
            continue

def read_file_as_space_separated_data(filepath: os.PathLike) -> Dict:
    with open(filepath, 'r') as f:
        lines = f.read().splitlines()
        lines = [[v.strip() for v in l.strip().split(' ')] for l in lines]
        data = {l[0]: l[1:] for l in lines}
    return data

def download_video(video_id, download_path, resolution: int=None, video_format="mp4"):
    video_selection = f"bestvideo[ext={video_format}]" + (f"[height<={resolution}]" if resolution else "")
    video_command = [
        "yt-dlp",
        f"https://youtube.com/watch?v={video_id}",
        "--quiet",
        "-f", video_selection,
        "--output", download_path,
        "--no-continue",
        "--no-part"
    ]
    return subprocess.call(video_command) == 0

def get_video_resolution(video_path: os.PathLike) -> int:
    command = ' '.join([
        "ffprobe",
        "-v", "error",
        "-select_streams", "v:0",
        "-show_entries", "stream=height",
        "-of", "csv=p=0",
        video_path
    ])
    process = Popen(command, stdout=PIPE, shell=True)
    (output, err) = process.communicate()
    process.wait()
    return int(output.strip())

def cut_and_crop_video(raw_video_path, output_path, start, end, crop: List[int]):
    x, out_w, y, out_h = crop
    video_command = ' '.join([
        "ffmpeg", "-i", raw_video_path,
        "-ss", str(start), "-to", str(end),
        "-filter:v", f"crop={out_w}:{out_h}:{x}:{y}",
        "-c:a", "copy",
        output_path
    ])
    return subprocess.call(video_command, shell=True) == 0

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download HDTF dataset using yt-dlp")
    parser.add_argument('-s', '--source_dir', type=str, default='HDTF_dataset', help='Path to the directory with the dataset')
    parser.add_argument('-o', '--output_dir', type=str, required=True, help='Where to save the videos?')
    parser.add_argument('-w', '--num_workers', type=int, default=8, help='Number of workers for downloading')
    args = parser.parse_args()

    download_hdtf(args.source_dir, args.output_dir, args.num_workers)
