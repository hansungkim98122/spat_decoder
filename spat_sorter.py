import pickle
from spat_decoder import spat_decoder
import time
import os
from tqdm import tqdm

class SPAT():
    def __init__(self):
        self.region = 0
        self.id = 0
        self.moy = 0

#Data parsing function
def parse_spat(dict):
    parsed_data = SPAT()
    #Dict is the decoded SPaT data in dictionary format
    raw_data = dict['intersections'][0] #Take the first element in a list of dictionary with a length of 1

    #Parse ID
    #Region 0 is assumed
    parsed_data.id = raw_data['id']['id']

    return parsed_data

#Data directory
root_dir = '/home/hansung/spat_data/' #Enter the directory of all the folders by date
folder_list = []
for it in os.scandir(root_dir):
    if it.is_dir():
        folder_list.append(it.path)

for i, folder_dir in enumerate(tqdm(folder_list)):
    data_dir = folder_dir + '/spat_data_0.pickle' #This filename is consistent
    exp_dir = folder_dir + '/decoded_data/'
    if not os.path.exists(exp_dir):
        os.makedirs(exp_dir)

    #Unpickling SPaT recorded data
    t = time.time()
    with open(data_dir, "rb") as f:
        try:
            while True:
                
                data = pickle.load(f,encoding='bytes')
                decoded = spat_decoder(data)
                parsed_data = parse_spat(decoded)

                #The raw data is already sorted chronologically
                filename = exp_dir + str(parsed_data.id) + '.pickle'

                #Export 
                with open(filename,'ab') as fw:
                    pickle.dump(decoded,fw) 
                
        except EOFError:
            pass 
    elapsed_t = time.time() - t
    print('DECODING AND EXPORTING FINISHED!\n')
    print(f'Decoding time: {elapsed_t} sec')


