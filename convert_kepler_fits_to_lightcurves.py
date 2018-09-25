from astropy.io import fits
from sklearn.externals import joblib
from subprocess import call
from tqdm import tqdm
from argparse import ArgumentParser

ap = ArgumentParser()
ap.add_argument('-f', '--filename', type=str, required=True, help="The file with a bash script of wget entries for Simulated Kepler Lightcurves.")
ap.add_argument('-o', '--outputdir', type=str, required=False, default='./', help="The directory which to push the data final gunzip files.")
ap.add_argument('-c', '--chunk', type=bool, nargs='?', required=False, default=False, help="The directory which to push the data final gunzip files.")
ap.add_argument('-nc', '--numchunks', type=int, required=False, default=100, help="Number of chunks  into which to segment stack of files.")

args = vars(ap.parse_args())

# n_calls = 140975
filename = args['filename'] # 'injected-light-curves-dr25-inj3.bat'
outputdir = args['outputdir']
do_chunk = args['chunk']
n_chunks = args['numchunks']

# filename = 'injected-light-curves-dr25-inj3.last10.bat'
str1000 = str(1000)
len1000 = len(str1000)

if do_chunk:
    with open(filename,'r') as filein:
        n_lines = len(filein.readlines())
    
    n_files_per_chunk = n_lines // n_chunks
    
    i_chunk = 0
    output_dir0 = output_dir

with open(filename,'r') as filein:
    for kl, line in tqdm(enumerate(filein.readlines())):
        if line[0] != '#' and line[:4] =='wget':
            if do_chunk and (kl % n_dir_push == 0): 
                output_dir = output_dir0 + '_{0:04}'.format(i_chunk)
                i_chunk = i_chunk + 1
            
            line_splits = line.replace('wget','wget -c --no-check-certificate').split(' ')
            call(line_splits)
            
            fits_filename = line_splits[4]
            
            # Load and extract ONLY the part(s) that we want for feature injection into the network
            flux_now = fits.open(fits_filename)['INJECTED LIGHTCURVE'].data['PDCSAP_FLUX']
            
            save_filename = fits_filename.replace('.fits.gz', '.joblib.save')
            joblib.dump(flux_now, save_filename)
            
            call(['gzip','-S', '.gz', save_filename])
            call(['rm', '-rf', fits_filename])
