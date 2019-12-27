#!/usr/bin/env python
# coding: utf-8
import logging, json, sys
import concurrent.futures 
from multiprocessing import Process, Queue, cpu_count, current_process, Manager
import pytube
import ffmpeg

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] - %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)

def group_urls_task(queue_urls, result_dict):
    try:
        name, url = queue_urls.get(True, 0.05)
        result_dict[url] = None
        logger.debug("[%s] putting url [%s] in dictionary..." % (
            current_process().name, url))
    except queue.Empty:
        logging.error('Nothing to be done, queue is empty')

def crawl_task(url, name):
    """ Fetch video and audio files (1080p) and join them with ffmpeg """
    try:
        ########################################
        #  Download file streams from youtube  #
        ########################################
        youtube = pytube.YouTube(url)
        video = youtube.streams.filter(mime_type="video/mp4", res="1080p").all()[0]
        audio = youtube.streams.filter(mime_type="audio/mp4").all()[0]

        # filenames
        video_filename = name + ' - video'
        audio_filename = name + ' - audio'
       
        # Download files
        logger.info("[%s] fetching [%s] -- [%s] -- [%s]" % (
            current_process().name, video_filename, name, url))
        video.download(filename = video_filename)
        logger.info("[%s] fetching [%s] -- [%s] -- [%s]" % (
            current_process().name, audio_filename, name, url))
        audio.download(filename = audio_filename)

        #####################
        #  Post Processing  #
        #####################
        # Join Audio and Video
        filename_out = str(name) + str('.mp4')
        vs = video_filename + str('.mp4')
        aus = audio_filename + str('.mp4')
        logger.info("[%s] ffmpeg [%s] - join - [%s] - with - [%s] -> [%s]" % (
            current_process().name, name, vs, aus, filename_out ))
        video_stream = ffmpeg.input(vs)
        audio_stream = ffmpeg.input(aus)
        ffmpeg.output(audio_stream, video_stream, filename_out).run()
    except:
        logger.error(sys.exc_info()[0])
        raise
    finally:
        # redefine some variables in case of error 
        video_filename = name + ' - video'
        audio_filename = name + ' - audio'
        return (url, name, video_filename, audio_filename)


if __name__ == '__main__':

    video_urls = {
        '5_02 Calculus': 'https://youtu.be/ss3NSODhFJI',
        '5_03 Introduction': 'https://youtu.be/ss3NSODhFJI',
        '5_04 Derivates': 'https://youtu.be/mzEit_Oe13E',
        '5_05 Derivates trough Geometry': 'https://youtu.be/mzllBDdtjIg',
        '5_06 The Chain rule': 'https://youtu.be/-Ti33VDPA7s',
        '5_07 Derivatives of exponentials': 'https://youtu.be/QsEJGPVxsB8',
        '5_08 Implicit Differentiation': 'https://youtu.be/2cNiF9ebdL0',
        '5_09 Limits': 'https://youtu.be/5aL7Fn-9NnQ',
        '5_10 Integrals': 'https://youtu.be/gKZvmxtM79Q',
        '5_11 More on Integrals': 'https://youtu.be/OF3awXGM4k0',
        '5_12 The Taylor Series (Optional)': 'https://youtu.be/1BLFuSXfgMY',
        '5_13 Multivariable Chain Rule': 'https://youtu.be/wXDy3P6F_P8',
        '5_14 What is Neural Network': 'https://youtu.be/hYB4ku0kmW8',
        '5_15 Gradient Descent': 'https://youtu.be/f8PYXDsSBpM',
        '5_16 Backpropagation': 'https://youtu.be/Wr5qCQ48t8E',
        '5_17 Backpropagation and Calculus': 'https://youtu.be/_d52fwoXXd4',
        '5_18 Gradient Descent- Example': 'https://youtu.be/_K7lzXqyj8I',
        }

    number_of_cpus = cpu_count()
    manager = Manager()
    queue_urls = manager.Queue()
    
    for name, url in video_urls.items():
        queue_urls.put((name,url))
    result_dict = manager.dict()

    with concurrent.futures.ProcessPoolExecutor(max_workers=number_of_cpus) as group_link_processes:
        for i in range(queue_urls.qsize()):
            group_link_processes.submit(
                    group_urls_task, queue_urls, result_dict)

    with concurrent.futures.ProcessPoolExecutor(
            max_workers=number_of_cpus) as crawler_link_processes:
        future_tasks = {crawler_link_processes.submit(
            crawl_task, url, name): (name, url) for name, url in video_urls.items()}
        for future in concurrent.futures.as_completed(future_tasks):
            # result_dict[future.result()[0]] = future.result()[1]
            result_dict[future.result()[0]] = future.result()
 
    # Print result
    # print(json.dumps(dict(result_dict), sort_keys=False, indent=4, separators=(',', ': ')))
    # print(result_dict)



