import pickle
from spat_decoder import spat_decoder
import time

#Data directory
data_dir = "/home/hansung/spat_data/spat_data_0.pickle"

#Unpickling SPaT recorded data
with open(data_dir, "rb") as f:
    try:
        while True:
            t = time.time()
            data = pickle.load(f,encoding='bytes')
            decoded = spat_decoder(data)
            elapsed_t = time.time() - t
            # print(f'Decoding time: {elapsed_t} sec')
            print(decoded)
    except EOFError:
        pass 

print('Done')

#Export
