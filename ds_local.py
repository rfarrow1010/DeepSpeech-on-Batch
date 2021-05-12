#!/usr/bin/env python3

# intended to run a single audio file.
# splits it up into smaller audio files of desired length in secs
# to test, runs all locally

import sys, os, subprocess

def usage():
    print('''Usage:
    ./ds_local.py file.mp4 -m maxSnippetLength 
    Your machine must have ffmpeg. 
    If maxSnippetLength exceeds 895, the program will proceed
    as if the user had input 895. Must be an integer representing seconds.
    ''')
    exit(0)

def check_dependencies():
    '''
    Checks that this system has ffmpeg on it. If not, calls usage.
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

    # convert to .wav if not in .wav format
    # FFmpeg supports 100+ audio types, and will throw catchable errors if it cannot convert
    if ".wav" not in filename:
        try:
            os.system("ffmpeg -i " + filename + " " + filename[:-4] + ".wav")
            filename = filename[:-4] + ".wav"
        except Exception as e:
            print(e)
            exit(1)

    # split
    try:
        os.system("ffmpeg -i " + filename + " -f segment -segment_time " + str(snippet_len) + " -c copy dsfile%03d.wav -loglevel error")
    except Exception as e:
        print(e)
        exit(1)

    # split after an offset, used to implement correction
    # try:
    #     os.system("ffmpeg -i " + filename + " -f segment -segment_time " + str(snippet_len) + " -output_ts_offset " + str(snippet_len / 2) + " -c copy OFFdsfile%03d.wav -loglevel error")
    # except Exception as e:
    #     print(e)
    #     exit(1)
    return

def clean_working_directory():
    '''
    Cleans the working directory of any procedurally generated audio files.
    Returns void.
    '''

    os.system("rm dsfile*.wav dsfile*.txt output.txt")
    return

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

                # run locally
                os.system("deepspeech --model deepspeech-0.9.3-models.tflite --audio " + entry.name + " > " + entry.name[:-4] + ".txt")
                # result = subprocess.run([
                #     "deepspeech", 
                #     "--model", 
                #     "models/deepspeech-0.9.3-models.pbmm", 
                #     "--scorer",
                #     "models/deepspeech-0.9.3-models.scorer",
                #     "--audio",
                #     entry.name
                # ])
                # outfile = open(entry.name[:-4] + ".txt", "w")
                # outfile.write(result.stdout)

    # concatenate the outputs of the segments
    o = open("output.txt", "w")
    f_num = 0

    while os.path.exists("dsfile%03d.txt" % f_num):
        f = open("dsfile%03d.txt" % f_num, "r")
        o.write(f.read().strip() + " ")
        f.close()
        f_num += 1

    # for entry in os.scandir():
    #     if entry.is_file():
    #         print(entry.name)
    #         print(f_num)
    #         if "dsfile" in entry.name and str(f_num) + ".txt" in entry.name:
    #             f = open(entry.name, "r")
    #             o.write(f.read().strip() + " ")
    #             f.close()
    #             f_num += 1
    
    o.write("\n")
    o.close()

    # clean intermediate files
    os.system("rm dsfile*.wav dsfile*.txt")

if __name__ == "__main__":
    main()
