#!/usr/bin/env python3

# File intended to run for a single audio file, split it up into the desired amount of
# smaller audio files to be sent to the cloud for processing, and do whatever else needs
# to be done. Can be run multiple times by a higher level script if necessary for multiple
# input audio/video files.

import sys, subprocess

def usage():
    print('''Usage:
    ./ds_on_batch.py file.mp4 -m numberOfMachines 
    Your local machine must have ffmpeg and mp3info.
    ''')
    exit(0)


def check_dependencies():
    '''
    Checks that this system has ffmpeg and mp3info on it. If not,
    calls usage.
    '''
    try:
        subprocess.check_output(["which", "ffmpeg"])
        subprocess.check_output(["which", "mp3info"])
    except Exception as e:
        print(e)
        usage()


def split_audio(filename, num):
    '''
    Splits up the given audio file into the given number of equally sized
    smaller audio files using ffmpeg. The files will appear in the working
    directory and will be named procedurally. Returns void.
    '''
    # first, need to get the total length of the input file in seconds. can be done like this:
    # mp3info -p "%S\n" filename
    pass


def clean_working_directory():
    '''
    Cleans the working directory of any procedurally generated audio files.
    Returns void.
    '''
    # we are going to assume that the filenames can range from dsfile000.mp3 to dsfile999.mp3
    # NOTE: just a first draft, check that this works
    for i in range(1000):
        subprocess.run(["rm", f"dsfile{i:03d}.mp3"])


def main():
    # run script here
    # start by cleaning working directory and checking dependencies
    clean_working_directory()
    check_dependencies()

    # parse command line
    audio_filename = sys.argv[1]
    number_of_machines = int(sys.argv[3])
    

if __name__ == "__main__":
    main()