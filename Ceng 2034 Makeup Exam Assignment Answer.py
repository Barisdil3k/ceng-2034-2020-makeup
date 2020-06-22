# BARIŞ DİLEK-390709075
import hashlib
import multiprocessing
import os
import sys
import time
import urllib.request  # built-in library to download files
import uuid
from threading import Thread

REQUIRED_MEMORY_MB = 25  # minimum required amount of memory in Megabytes

HASH_BUFFER_SIZE = 32768  # buffer size in Bytes to use in hashing
MOVIE_FILE_PATH = None  # the path of the movie file, make it 'None' if doesn't exist

DOWNLOAD_FOLDER = 'files/'  # folder to download the files.
URLs = [
    "http://wiki.netseclab.mu.edu.tr/images/thumb/f/f7/MSKU-BlockchainResearchGroup.jpeg/300px-MSKU-BlockchainResearchGroup.jpeg",
    "https://upload.wikimedia.org/wikipedia/tr/9/98/Mu%C4%9Fla_S%C4%B1tk%C4%B1_Ko%C3%A7man_%C3%9Cniversitesi_logo.png",
    "https://upload.wikimedia.org/wikipedia/commons/thumb/c/c3/Hawai%27i.jpg/1024px-Hawai%27i.jpg",
    "http://wiki.netseclab.mu.edu.tr/images/thumb/f/f7/MSKU-BlockchainResearchGroup.jpeg/300px-MSKU-BlockchainResearchGroup.jpeg",
    "https://upload.wikimedia.org/wikipedia/commons/thumb/c/c3/Hawai%27i.jpg/1024px-Hawai%27i.jpg"
]  # the URLs that were specified in the homework documentation


def get_available_memory() -> int:
    # memory info is located in this file in Unix-like operating systems
    with open('/proc/meminfo', 'r') as mem:
        ret = {}
        free = 0
        for i in mem:
            sline = i.split()
            if str(sline[0]) == 'MemTotal:':
                ret['total'] = int(sline[1])
            elif str(sline[0]) in ('MemFree:', 'Buffers:', 'Cached:'):
                free += int(sline[1])
        return free


def wait_until_memory_is_available():
    while True:  # wait until memory is available
        available_memory_MB = get_available_memory() / 1000
        if available_memory_MB < REQUIRED_MEMORY_MB:  # if the available memory is less than the required memory
            print(
                "Not enough memory! Available: {} MB | Required: {} MB".format(available_memory_MB, REQUIRED_MEMORY_MB))
            time.sleep(1)  # wait a bit after checking again
        else:
            return  # program continues


def download_file(_url: str, _file_paths: list, file_name=None):
    try:
        # the example code in the question documentation was using the "requests" library
        # which is actually a third party library. however, the rules were indicating that
        # we are not allowed to use any third party dependency. thus, I'm using "urllib" which is built-in
        r = urllib.request.urlopen(_url)  # request the file

        if not os.path.exists(DOWNLOAD_FOLDER):  # create the directory if not exists
            os.makedirs(DOWNLOAD_FOLDER)

        # helpful variables to get the file format and full local path
        file_format = _url.split('.')[-1]
        local_path = DOWNLOAD_FOLDER + (file_name if file_name else str(uuid.uuid4())) + '.' + file_format

        status_code = r.getcode()
        if status_code == 200:  # if the request was successful
            with open(local_path, 'wb') as f:
                f.write(r.read())  # write its content to a file
        else:
            raise Exception("Failed ({}): {}".format(status_code, _url))
    except Exception as e:
        print(e)
    else:
        print("Successful ({}): {}".format(status_code, _url))
        _file_paths.append(local_path)


def child_process():
    print("\nThe ID of the child process: ", os.getpid())

    threads = []
    file_paths = []  # file path list to remove files if the parent dies before the child
    for URL in URLs:
        thread = Thread(target=download_file, args=(URL, file_paths))  # create the thread
        thread.start()  # start it
        threads.append(thread)  # add it to the threads list

    for thread in threads:  # wait until each thread is completed
        thread.join()

    # If the ID of the parent process is 1, it means the parent process has died.
    # On Windows, this method returns the same ID. (https://docs.python.org/3/library/os.html#os.getppid)
    # HOWEVER, it's still safe to check it this way since
    # os.fork() can only be used on Unix based operating systems. (https://docs.python.org/3/library/os.html#os.fork)
    if os.getppid() == 1:
        print("Apparently the parent process is dead somehow, so we are deleting the files we just downloaded.")
        for _path in file_paths:
            os.remove(_path)
        os._exit(-1)

    os._exit(0)


def get_sha256_hash(_file_path) -> str:
    sha256 = hashlib.sha256()  # SHA-256 hash function

    with open(_file_path, mode='rb') as f:  # open the file in read binary mode
        while True:
            data = f.read(HASH_BUFFER_SIZE)  # read a chunk of it
            if not data:
                break
            sha256.update(data)  # update the hash

    return sha256.hexdigest()  # return the hash


def dup_file_checker(_file_queue, _hash_set, _duplicate_files, _lock):
    while True:  # work until terminate sign is given
        _file_path = _file_queue.get()

        # terminate sign is 'None'
        if not _file_path:  # or "if _file_path is None"
            return

        file_hash = get_sha256_hash(_file_path)  # get its SHA-256 hash

        # this is to keep the hash set synchronized across all processes
        # so that we don't get unexpected results due to race conditions.
        _lock.acquire()
        if file_hash in _hash_set:
            os.remove(_file_path)
            _duplicate_files.append(_file_path)
        else:
            _hash_set.append(file_hash)
        _lock.release()


def multiprocessing_part():
    def start_and_append_new_process(_pool):
        _p = multiprocessing.Process(target=dup_file_checker,
                                     args=(file_queue, hash_set, duplicate_files, lock))
        _p.start()
        _pool.append(_p)

    core_count = multiprocessing.cpu_count()
    print("There are {} cores in this system.".format(core_count))

    manager = multiprocessing.Manager()

    # A semaphore object to prevent race conditions
    # and have the hash set synchronized across all processes.
    # Without this, there's a chance to miss some duplicate files
    lock = multiprocessing.Lock()

    file_queue = manager.Queue(core_count)
    hash_set = manager.list()
    duplicate_files = manager.list()

    pool = []  # process pool
    # create "core_count" number of processes
    for _ in range(core_count):
        start_and_append_new_process(pool)

    print("{} processes have been created to check duplicate files...".format(len(pool)))

    # add the files to the queue
    # our queue size is also equal to the core count
    for file in os.scandir(DOWNLOAD_FOLDER):
        file_queue.put(file.path)

    # adding "core_count" number of "None"s
    # to tell the each process to terminate after there is no more file left
    for _ in range(core_count):
        file_queue.put(None)

    while pool:  # until all processes are done
        p = pool.pop()
        p.join(30)  # timeout after 30 seconds

        # if it timed out but still not yet terminated
        # https://docs.python.org/3/library/multiprocessing.html#multiprocessing.Process.exitcode
        if p.exitcode is None:
            p.close()  # close() is a safer option

        elif p.exitcode > 0:  # if it didn't end successfully
            print("Recreating {} because it didn't end successfully.".format(p.name))
            start_and_append_new_process(pool)

        else:
            pass

    print("{} duplicate files have been deleted.".format(len(duplicate_files)))


if __name__ == '__main__':
    # here we used this instead of os.name ('os.name != posix')
    # because POSIX does not exactly mean a Unix-like operating system.
    if not sys.platform.startswith('linux'):  # if not on Linux
        print('Your operating system does not support this program! Please run it on a Unix-like operating system.')
        exit()

    wait_until_memory_is_available()

    print("The ID of the parent process: ", os.getppid())
    print("The ID of the current process: ", os.getpid())

    child = os.fork()
    if child == 0:
        child_process()

    child_exit_status = os.wait()
    print("Child process has exited with status {}.\n".format(child_exit_status[1]))

    multiprocessing_part()

    if MOVIE_FILE_PATH:
        movie_hash = get_sha256_hash(MOVIE_FILE_PATH)
        print("The hash of the movie using {} byte chunks: {}".format(HASH_BUFFER_SIZE, movie_hash))

    print("\nExiting...")
