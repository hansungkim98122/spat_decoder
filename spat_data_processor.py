from cmath import nan
import pandas as pd
import pickle
import os
from tqdm import tqdm

class timing():
    def __init__(self):
        self.startTime = 0
        self.minEndTime = 0
        self.maxEndTime = 0
        self.likelyTime = 0
        self.confidence = 0.0
        self.nextTime = 0
class state_time_speed():
    def __init__(self):
        self.eventState = ''
        self.timing = timing()
        self.speeds = -1
class State():
    def __init__(self):
        self.signalGroup = 0
        self.state_time_speed = state_time_speed()
class SPAT():
    def __init__(self):
        self.region = 0
        self.id = 0
        self.status = nan
        self.moy = 0
        self.timeStamp = 0
        self.states = []
        self.num_signals = 0

#Data parsing function
def parse_spat(dict):
    parsed_data = SPAT()
    #Dict is the decoded SPaT data in dictionary format
    raw_data = dict['intersections'][0] #Take the first element in a list of dictionary with a length of 1

    #Parse ID
    #Region 0 is assumed
    parsed_data.id = raw_data['id']['id']

    #Minute of the Year and timeStamp
    parsed_data.moy = raw_data['moy']
    parsed_data.timeStamp = raw_data['timeStamp']
    flag = False
    #States
    for state in raw_data['states']: 
        
        temp = State()
        temp.signalGroup = state['signalGroup']
        parsed_data.num_signals = len(raw_data['states'])

        if len(state['state-time-speed']) != 1:
            print('ERROR: Check \'state-time-speed\' length')
        else:
            curr_state_time_speed = state['state-time-speed'][0]
            if 'speeds' in curr_state_time_speed:
                temp.state_time_speed.speeds = curr_state_time_speed['speeds']
            temp.state_time_speed.eventState = curr_state_time_speed['eventState']

            temp.state_time_speed.timing.startTime = curr_state_time_speed['timing']['startTime']
            temp.state_time_speed.timing.minEndTime = curr_state_time_speed['timing']['minEndTime']
            temp.state_time_speed.timing.maxEndTime = curr_state_time_speed['timing']['maxEndTime']
            if 'likelyTime' in curr_state_time_speed['timing']:
                temp.state_time_speed.timing.likelyTime = curr_state_time_speed['timing']['likelyTime']
                flag = True
            if 'confidence' in curr_state_time_speed['timing']:
                temp.state_time_speed.timing.confidence = curr_state_time_speed['timing']['confidence']
                flag = True
            if 'nextTime' in curr_state_time_speed['timing']:
                temp.state_time_speed.timing.nextTime = curr_state_time_speed['timing']['nextTime']
                flag = True
            if flag:
                print('ERROR: Check \'timing\' length')
            
            parsed_data.states.append(temp)
    

    return parsed_data

def moy_to_pst(moy,timeStamp):
    # print(lb.DateTime.from_moy(moy))
    # moy = moy - 480
    daysInMonths = [31,28,31,30,31,30,31,31,30,31,30,31]
    days = moy // 1440 + 1
    remaining_days = days
    for ind, curr_daysinmonth in enumerate(daysInMonths):
        if remaining_days - curr_daysinmonth < 0:
            month = ind + 1
            break
        else:
            remaining_days -= curr_daysinmonth
    day = remaining_days
    hour = int((moy / 60) % 24)
    minute = int(moy % 60)
    # print(f'{month}/{day}/2022 {hour}:{minute}:{timeStamp/1000} (UCT)')
    return month,day,hour,minute, timeStamp/1000


#Data directory
root_dir = "/home/hansung/spat_data/"
folder_list = []
for it in os.scandir(root_dir):
    if it.is_dir():
        folder_list.append(it.path)

for i, folder_dir in enumerate(tqdm(folder_list)):
    file_dir = folder_dir + '/decoded_data/'
    # list to store files
    res = []
    # Iterate directory
    for file in os.listdir(file_dir):
        # check only text files
        if file.endswith('.pickle'):
            res.append(file)

    for i, filename in enumerate(tqdm(res)):
        data_dir = file_dir + filename

        #Unpickling SPaT sorted data
        with open(data_dir, "rb") as f:
            try:
                run_once = 0
                while True:
                    export_data = {'ID': [], 'timeStamp': [],'Month': [], 'Day': [], 'Hour': [], 'Minute': [], 'Second': [], 'signalGroup': [], 'eventState': [], 'speeds': [], 'startTime':[],'minEndTime':[],'maxEndTime':[]}
                    decoded_data = pickle.load(f)
                    parsed_data = parse_spat(decoded_data)
                    # print(decoded_data)
                    month, day, hour, minute, second = moy_to_pst(parsed_data.moy,parsed_data.timeStamp)
                    # export_data['ID'].append(parsed_data.id)
                    # export_data['Month'].append(month); export_data['Day'].append(day); export_data['Hour'].append(hour); export_data['Minute'].append(minute); export_data['Second'].append(second)
                    for state in parsed_data.states:
                        export_data['ID'].append(parsed_data.id)
                        export_data['timeStamp'].append(parsed_data.timeStamp)
                        export_data['Month'].append(month); export_data['Day'].append(day); export_data['Hour'].append(hour); export_data['Minute'].append(minute); export_data['Second'].append(second)
                        export_data['signalGroup'].append(state.signalGroup)
                        export_data['eventState'].append(state.state_time_speed.eventState)
                        export_data['speeds'].append(state.state_time_speed.speeds)
                        export_data['startTime'].append(state.state_time_speed.timing.startTime)
                        export_data['minEndTime'].append(state.state_time_speed.timing.minEndTime)
                        export_data['maxEndTime'].append(state.state_time_speed.timing.maxEndTime)

                    df = pd.DataFrame.from_dict(export_data)    
                    if run_once == 0:
                        df.to_csv(file_dir + str(parsed_data.id)+'.csv', mode='w', index=False, header=True)
                        run_once = 1
                    else:
                        df.to_csv(file_dir + str(parsed_data.id)+'.csv', mode='a', index=False, header=False)
                            
            except EOFError:
                pass 

