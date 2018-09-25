from argparse import ArgumentParser
from astropy.io import fits
from multiprocess import Pool, cpu_count
from os import path, mkdir
from sklearn.externals import joblib
from subprocess import call
from tqdm import tqdm

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

with open(filename,'r') as filein: n_lines = len(filein.readlines())

print('[INFO] Loaded file {} with {} lines.'.format(filename, n_lines))

if do_chunk:
    n_files_per_chunk = n_lines // n_chunks
    
    i_chunk = 0
    
    if outputdir[-1] == '/': outputdir = outputdir[:-1]
    
    outputdir0 = outputdir
    
    print('[INFO] Chunking flagged with {} segments and {} lines per segment.'.format(n_chunks, n_files_per_chunk))

if not path.exists(outputdir): mkdir(outputdir)

def grab_and_process_one_file(line, outputdir, do_chunk=False, i_chunk=0, n_files_per_chunk=100):
    if line[0] != '#' and line[:4] =='wget':
        
        line_splits = line.replace('wget','wget -c --no-check-certificate').split(' ')
        fits_filename = line_splits[4]
        
        call(line_splits)
        
        # Load and extract ONLY the part(s) that we want for feature injection into the network
        flux_now = fits.open(fits_filename)['INJECTED LIGHTCURVE'].data['PDCSAP_FLUX']
        
        save_filename = fits_filename.replace('.fits.gz', '.joblib.save')
        joblib.dump(flux_now, save_filename)
        
        call(['gzip','-S', '.gz', save_filename])
        call(['mv', save_filename + '.gz', outputdir])
        call(['rm', '-rf', fits_filename])

with open(filename,'r') as filein:
    print('[INFO] Begin Proecessing of File {}.'.format(filename))
    # for kl, line in tqdm(enumerate(filein.readlines()), total=n_lines):
    if do_chunk and (kl % n_files_per_chunk == 0): 
        outputdirs = []
        
        for k in range(n_chunks): outputdirs.extend([outputdir0 + '_{0:04}/'.format(k)]*n_files_per_chunk)
        
        for outputdir in outputdirs: if not path.exists(outputdir): mkdir(outputdir)
    
    else:
        outputdirs = [outputdir]*n_lines
    
    pool = Pool(cpu_count())
    pool.starmap(grab_and_process_one_file, zip(filein.readlines(), outputdirs))
    pool.close()
    pool.join()
