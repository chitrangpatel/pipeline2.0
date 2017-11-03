import StringIO
import os
import pipeline_utils
import config.basic
################################################################
# Result Uploader Configuration
################################################################
def version_num():
    """Compute version number and return it as a string.
    """
    prestohash = pipeline_utils.execute("git rev-parse HEAD", \
                            dir=os.getenv('PRESTO'))[0].strip()
    pipelinehash = pipeline_utils.execute("git rev-parse HEAD", \
                            dir=config.basic.pipelinedir)[0].strip()
    psrfits_utilshash = pipeline_utils.execute("git rev-parse HEAD", \
                            dir=config.basic.psrfits_utilsdir)[0].strip()
    vernum = 'PRESTO:%s;PIPELINE:%s;PSRFITS_UTILS:%s' % \
                            (prestohash, pipelinehash, psrfits_utilshash)
    return vernum

# Whether to upload zerodm candidates to Cornell DB
upload_zerodm_periodicity = False
upload_zerodm_singlepulse = True

# Directory on the FTP server to upload PFDs and singlepulse files (do not change unless asked by Adam)
pfd_ftp_dir = 'pfd9/PRESTO4'
spd_ftp_dir = 'spd9/PRESTO4'
sp_ftp_dir = 'singlePulse9/PRESTO4'

import upload_check
upload_check.upload.populate_configs(locals())
upload_check.upload.check_sanity()
