#!/usr/bin/env python3

# File intended to run for a single audio file, split it up into the desired amount of
# smaller audio files to be sent to the cloud for processing, and do whatever else needs
# to be done. Can be run multiple times by a higher level script if necessary for multiple
# input audio/video files.

import sys, os, subprocess

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
        # subprocess.check_output(["which", "mp3info"])
    except Exception as e:
        print(e)
        usage()


def split_audio(filename, snippet_len):
    '''
    Splits up the given audio file into
    smaller audio files using ffmpeg. The files will appear in the working
    directory and will be named procedurally. Returns void.
    '''
    # first, need to get the total length of the input file in seconds. can be done like this:
    # mp3info -p "%S\n" filename
    # NOTE: if we just split everything up into 1 or 2-minute segments, we might not need mp3info
    # after all

    # https://unix.stackexchange.com/questions/280767/how-do-i-split-an-audio-file-into-multiple

    # TODO: make sure we get audio sent up in .WAV format, that is the only kind DeepSpeech can process
    # seems like if the input is .wav, the outputs will be .wav as well, so we just need to make sure
    # we supply .wav files
    # so we could simply stipulate that this only takes .wav files or we could try to introduce
    # automatic file conversion as part of the project
    subprocess.run(f"ffmpeg -i {filename} -f segment -segment_time {snippet_len} -c copy dsfile%03d.wav")


def clean_working_directory():
    '''
    Cleans the working directory of any procedurally generated audio files.
    Returns void.
    '''
    # we are going to assume that the filenames can range from dsfile000.mp3 to dsfile999.mp3
    # NOTE: just a first draft, check that this works
    # this will also have to clean out the input S3 bucket

    
    for i in range(1000):
        subprocess.run(["rm", f"dsfile{i:03d}.wav"])


def main():
    # run script here
    # start by cleaning working directory and checking dependencies
    clean_working_directory()
    check_dependencies()

    # parse command line
    try:
        audio_filename = sys.argv[1]
        max_snippet_len = int(sys.argv[3])
    except:
        usage()

    # might want to change this; I don't know what the runtime is on these things
    if max_snippet_len > 895:
        max_snippet_len = 895

    split_audio(audio_filename, max_snippet_len)

    # go through every item in local directory, operate on any file that contains 
    # the dsfile prefix
    # important that this local directory stay clean for this reason
    for entry in os.scandir():
        if entry.is_file():
            if "dsfile" in entry:
                # send it up to the S3 bucket
                pass 

    # now we go fetch the results from the output S3 bucket
    # maybe the simplest, albeit naive, way of doing this is to repeatedly ping the bucket for each
    # file until it appears and once it does, grab it
    # after that happens, recompile the results



    # thoughts below

    # thinking the easiest way of doing this is to use AWS Lambda and chop up the 
    # audio files into user-defined pieces, max 14 minutes, 55 seconds to account for Lambda timeout
    # we can then upload each of those to S3 and trigger the Lambda function, which will run DeepSpeech
    # on it and send the output to another bucket, from which our client can then fetch the output
    # example of something similar:
    # https://docs.aws.amazon.com/lambda/latest/dg/with-s3-example.html
    # we just need to capture the output in a text file and then put it into the output S3 bucket like so:
    # https://stackoverflow.com/questions/49163099/writing-a-file-to-s3-using-lambda-in-python-with-aws
    # our output bucket is on Ryan's account and I'll fetch the details at some point

    # this guide might also be useful:
    # https://docs.aws.amazon.com/pinpoint/latest/developerguide/tutorials-importing-data-create-python-package.html

    # one advantage of doing it this way is that ffmpeg will inadvertently generate some number of files
    # which we will then send up to the S3 bucket, triggering an identical amount of Lambda function calls.
    # in that way, we can still control the parralelization of the system, just in a different manner

    # however, it might not be possible to do this in a Lambda function unless we use the Python streaming
    # version of DeepSpeech (which I don't understand). so we might have to just set up a bunch of EC2 
    # machines with DeepSpeech on them in order to set up a WorkQueue or something
    # or better yet, just do all of this on Notre Dame's HTCondor. That's starting to seem like an easier
    # solution

    # Thain's advice for however we end up doing this:
    # we need to package all of the dependencies (DeepSpeech itself, model, maybe scorer) and send them to the
    # machines that will be doing the execution
    # I'm pretty sure I have the dependencies packaged already. If it's not done right, he said to use conda
    # to create a virtualenv, conda install deepspeech (or conda install pip -> pip install deepspeech),
    # then zip that virtualenv and send it up to the the machine we want to use
    # set the PATH and the PYTHONPATH such that the machine importing the package will look in the package
    # for the executable
    # practice doing this on the student machines first, then do it on Lambda (or Condor, EC2, whatever)
    

if __name__ == "__main__":
    main()