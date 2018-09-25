from astropy.io import fits
from sklearn.externals import joblib
from subprocess import call
from tqdm import tqdm
from argparse import ArgumentParser

ap = ArgumentParser()
ap.add_arg('-f', '--filename', type=str, required=True, help="The file with a bash script of wget entries for Simulated Kepler Lightcurves.")

args = vars(ap.parse_args())

# n_calls = 140975
filename = args['filename'] # 'injected-light-curves-dr25-inj3.bat'
# filename = 'injected-light-curves-dr25-inj3.last10.bat'
str1000 = str(1000)
len1000 = len(str1000)

n_dir_push = 1000
with open(filename,'r') as filein:
    for kl, line in tqdm(enumerate(filein.readlines())):
        if line[0] != '#' and line[:4] =='wget':
            # if kl % n_dir_push: os.mkdir('0'*(len1000 - len(str(kl))))
            
            line_splits = line.replace('wget','wget -c --no-check-certificate').split(' ')
            call(line_splits)
            
            fits_filename = line_splits[4]
            
            # Load and extract ONLY the part(s) that we want for feature injection into the network
            flux_now = fits.open(fits_filename)['INJECTED LIGHTCURVE'].data['PDCSAP_FLUX']
            
            save_filename = fits_filename.replace('.fits.gz', '.joblib.save')
            joblib.dump(flux_now, save_filename)
            
            call(['gzip','-S', '.gz', save_filename])
            
            call(['rm', '-rf', fits_filename])
