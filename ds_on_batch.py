#!/usr/bin/env python3

# File intended to run for a single audio file, split it up into the desired amount of
# smaller audio files to be sent to the cloud for processing, and do whatever else needs
# to be done. Can be run multiple times by a higher level script if necessary for multiple
# input audio/video files.

from work_queue import *
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

    # set up WorkQueue; using this port number for now, may be changed
    try:
        q = WorkQueue(debug_log="debug.log", name="BigAudio", port=9037)
    except:
        print("Instantiation of WorkQueue failed!")
        exit(1)

    print(f"listening on port {q.port}")

    # run deepspeech on every .wav file that we produced from segmenting
    for entry in os.scandir():
        if entry.is_file():
            if "dsfile" in entry.name and ".wav" in entry.name:
                
                # create a task for this file and send it to WorkQueue
                audio = entry.name 
                model = "./models/deepspeech-0.9.3-models.tflite"
                # scorer = "./models/deepspeech-0.9.3-models.scorer"
                outfile = entry.name[:-4] + ".txt"
                command = "unzip models/deepspeech-venv-tflite.zip -d models/; export PATH=models/deepspeech-venv-tflite/bin:$PATH; ./models/deepspeech-venv-tflite/bin/deepspeech --model models/deepspeech-0.9.3-models.tflite --audio " + entry.name + " > " + entry.name[:-4] + ".txt"
                # command = "unzip models/deepspeech-venv-tflite.zip -d models/; export PATH=models/deepspeech-venv-tflite/bin:$PATH; deepspeech --model models/deepspeech-0.9.3-models.pbmm --scorer models/deepspeech-0.9.3-models.scorer --audio " + entry.name + " > " + entry.name[:-4] + ".txt"
                executable = "models/deepspeech-venv-tflite.zip"

                t = Task(command)

                # specify input/output files; cache the executable, the model, and the scorer only
                t.specify_file(audio, audio, WORK_QUEUE_INPUT, cache=False)
                t.specify_file(executable, executable, WORK_QUEUE_INPUT, cache=True)
                t.specify_file(model, model, WORK_QUEUE_INPUT, cache=True)
                # t.specify_file(scorer, scorer, WORK_QUEUE_INPUT, cache=True)
                t.specify_file(outfile, outfile, WORK_QUEUE_OUTPUT, cache=False)

                taskid = q.submit(t)
                print(f"submitted task #{taskid}")

    print("")
    print("waiting for tasks to complete...")

    while not q.empty():
        t = q.wait(5)
        if t:
            print(f"task #{t.id} complete with return code {t.return_status}")
            if t.output:
                print("DEBUG INFO" + t.output + "\n")
            if t.return_status != 0:
                # task failed; handle error if we want
                pass

    print("all WorkQueue tasks complete!")

    # concatenate the outputs of the segments
    o = open("output.txt", "w")
    f_num = 0

    while os.path.exists("dsfile%03d.txt" % f_num):
        f = open("dsfile%03d.txt" % f_num, "r")
        o.write(f.read().strip() + " ")
        f.close()
        f_num += 1
    
    o.write("\n")
    o.close()

    # clean intermediate files
    # os.system("rm dsfile*.wav dsfile*.txt")

if __name__ == "__main__":
    main()


# thoughts below

    # Thain's advice for however we end up doing this:
    # we need to package all of the dependencies (DeepSpeech itself, model, maybe scorer) and send them to the
    # machines that will be doing the execution
    # I'm pretty sure I have the dependencies packaged already. If it's not done right, he said to use conda
    # to create a virtualenv, conda install deepspeech (or conda install pip -> pip install deepspeech),
    # then zip that virtualenv and send it up to the the machine we want to use
    # set the PATH and the PYTHONPATH such that the machine importing the package will look in the package
    # for the executable
    # then it's a simple matter of calling the command line executable like
    # deepspeech --model blahblah.tflite --audio blahblah.wav 
    # and grabbing the resulting output to be recompiled in this program
    # practice doing this on the student machines first, then do it on Lambda (or Condor, EC2, whatever)