'''
© 2022 Hansung Kim <hansung@berkeley.edu>
SPAT Decoder per SAE-J2735-2020 standard
'''
import asn1tools
import os

'''
Before using this decoder, complete the following steps:
1) Download the .zip file from https://www.sae.org/standards/content/j2735asn_202007/
2) Extract the contents into a folder
'''
dir = '/home/hansung/Research/spat/test/SAE-J2735-2020/' # Enter the folder directory that contain the extracted files(.asn)

#Compiling ASN FILES

#List of .asn filenames to compile
filenames = [dir + filename for filename in os.listdir(dir) if filename.endswith('.asn')]

#Compile (Compiling takes long, so it should only be done once)
spat = asn1tools.compile_files(filenames,'uper')


def spat_decoder(data):
    '''
    dir: directory of the extracted files in str
    data: payload to decode(byte object)
    '''

    #Decoding
    '''
    The decoding occurs in two layers:
    1) Decoding as "MessageFrame" which is the outermost type that wraps the SPAT message as specified in SAE-J2735-2020 V2X DSRC message dictionary.
    2) The 'value' of the decoded message in (1) is decoded as 'SPAT' type to extract the usable information such as intersection states, timing, moy, and etc.
    '''
    decoded = spat.decode('SPAT', spat.decode('MessageFrame', data)['value'])

    return decoded
