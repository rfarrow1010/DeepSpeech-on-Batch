#!/usr/bin/env python3

# intended to run a single audio file.
# splits it up into smaller audio files of desired length in secs
# to test, runs all locally

import sys, os, subprocess
import deepspeech

def usage():
    print('''Usage:
    ./ds_on_batch.py file.mp4 -m maxSnippetLength 
    Your local machine must have ffmpeg. 
    If maxSnippetLength exceeds 895, the program will proceed
    as if the user had input 895. Must be an integer representing seconds.
    ''')
    exit(0)

def check_dependencies():
    '''
    Checks that this system has ffmpeg and mp3info on it. If not,
    calls usage.
    '''
    try:
        subprocess.check_output(["which", "ffmpeg"])
    except Exception as e:
        print(e)
        usage()

def split_audio(filename, snippet_len):
    '''
    Splits up the given audio file into
    smaller audio files using ffmpeg. The files will appear in the working
    directory and will be named procedurally. Returns void.
    '''

    os.system("ffmpeg -i " + filename + " -f segment -segment_time " + str(snippet_len) + " -c copy dsfile%03d.wav")

def clean_working_directory():
    '''
    Cleans the working directory of any procedurally generated audio files.
    Returns void.
    '''

    os.system("rm dsfile*.wav dsfile*.txt output.txt")

def main():
    clean_working_directory()
    check_dependencies()

    # parse command line
    try:
        audio_filename = sys.argv[1]
        max_snippet_len = int(sys.argv[3])
    except:
        usage()

    # limit segment length
    if max_snippet_len > 895:
        max_snippet_len = 895

    split_audio(audio_filename, max_snippet_len)

    # run deepspeech on every .wav file that we produced from segmenting
    for entry in os.scandir():
        if entry.is_file():
            if "dsfile" in entry.name and ".wav" in entry.name:
                os.system("deepspeech --model models/deepspeech-0.9.3-models.pbmm --scorer models/deepspeech-0.9.3-models.scorer --audio " + entry.name + " > " + entry.name[:-4] + ".txt")
                # send it up to the S3 bucket
    
    # concatenate the outputs of the segments
    o = open("output.txt", "w")
    for entry in os.scandir():
        if entry.is_file():
            if "dsfile" in entry.name and ".txt" in entry.name:
                f = open(entry.name, "r")
                o.write(f.read().strip() + " ")
                f.close()
    
    o.write("\n")
    o.close()

    # clean intermediate files
    os.system("rm dsfile*.wav dsfile*.txt")

if __name__ == "__main__":
    main()