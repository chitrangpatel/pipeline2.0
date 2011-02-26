import os
import warnings
import traceback
import glob
import sys
import datafile

import header
import candidate_uploader
import diagnostic_uploader
import jobtracker
import database
import config.upload
import config.basic

# Suppress warnings produced by uploaders
# (typically because data, weights, scales, offsets are missing
#       from PSRFITS files)
warnings.filterwarnings("ignore", message="Can't find the .* column")

def run():
    """
    Drives the process of uploading results of the completed jobs.

    """
    query = "SELECT * FROM job_submits " \
            "WHERE status='processed'"
    processed_submits = jobtracker.query(query)
    print "Found %d processed jobs waiting for submission" % \
                len(processed_submits)
    for submit in processed_submits:
        upload_results(submit)


def upload_results(job_submit):
    """
    Uploads Results for a given submit.

        Input:
            job_submit: A row from the job_submits table.
                Results from this job submission will be
                uploaded.

        Output:
            None
    """
    print "Attempting to upload results"
    print "\tJob ID: %d, Job submission ID: %d" % \
            (job_submit['job_id'], job_submit['id'])
    try:
        db = database.Database('common-copy', autocommit=False)
        # Prepare for upload
        dir = job_submit['output_dir']
        fitsfiles = get_fitsfiles(job_submit)
        data = datafile.autogen_dataobj(fitsfiles)

        # Upload results
        header_id = header.upload_header(fitsfiles, dbname=db)
        print "\tHeader ID: %d" % header_id
        candidate_uploader.upload_candidates(header_id, \
                                             config.upload.version_num, \
                                             dir, dbname=db)
        print "\tCandidates uploaded."
        diagnostic_uploader.upload_diagnostics(data.obs_name, 
                                             data.beam_id, \
                                             config.upload.version_num, \
                                             dir,dbname=db)
        print "\tDiagnostics uploaded."
    except (header.HeaderError, \
            candidate_uploader.PeriodicityCandidateError, \
            diagnostic_uploader.DiagnosticError):
        # Parsing error caught. Job attempt has failed!
        exceptionmsgs = traceback.format_exception(*sys.exc_info())
        errormsg  = "Error while checking results!\n\n"
        errormsg += "".join(exceptionmsgs)
        
        sys.stderr.write("Error while checking results!\n")
        sys.stderr.write("Database transaction will not be committed.\n")
        sys.stderr.write("\t%s" % exceptionmsgs[-1])

        queries = []
        queries.append("UPDATE job_submits " \
                       "SET status='upload_failed', " \
                            "details='%s', " \
                            "updated_at='%s' " \
                       "WHERE id=%d" % \
                    (errormsg, jobtracker.nowstr(), job_submit['id']))
        queries.append("UPDATE jobs " \
                       "SET status='failed', " \
                            "details='%s', " \
                            "updated_at='%s' " \
                       "WHERE id=%d" % \
                    (errormsg, jobtracker.nowstr(), job_submit['job_id']))
        jobtracker.query(queries)
        
        # Rolling back changes. 
        db.rollback()
    except database.DatabaseConnectionError, e:
        # Connection error while uploading. We will try again later.
        sys.stderr.write(str(e))
        sys.stderr.write("\tWill re-try.\n")
    except:
        # Unexpected error!
        sys.stderr.write("Unexpected error!\n")
        sys.stderr.write("\tRolling back DB transaction and re-raising.\n")
        
        # Rolling back changes. 
        db.rollback()
        raise
    else:
        # No errors encountered. Commit changes to the DB.
        db.commit()

        # Update database statuses
        queries = []
        queries.append("UPDATE job_submits " \
                       "SET status='uploaded', " \
                            "details='Upload successful', " \
                            "updated_at='%s' " \
                       "WHERE id=%d" % 
                       (jobtracker.nowstr(), job_submit['id']))
        queries.append("UPDATE jobs " \
                       "SET status='uploaded', " \
                            "details='Upload successful', " \
                            "updated_at='%s' " \
                       "WHERE id=%d" % \
                       (jobtracker.nowstr(), job_submit['job_id']))
        jobtracker.query(queries)

        print "Results successfully uploaded\n"

        if config.basic.delete_rawdata:
            clean_up(job_submit)

def get_fitsfiles(job_submit):
    """Find the fits files associated with this job.
        There should be a single file in the job's result
        directory.

        Input:
            job_submit: A row from the job_submits table.
                A list of fits files corresponding to the submit
                are returned.
        Output:
            fitsfiles: list of paths to *.fits files in results
                directory.
    """
    return glob.glob(os.path.join(job_submit['output_dir'], "*.fits"))

def clean_up(job_submit):
    """
    Deletes raw files for a given job_row.

        Input:
            job_submit: A row from the job_submits table.
                The files associated to this job will be removed.
        Outputs:
            None
    """
    downloads = jobtracker.query("SELECT downloads.filename " \
                                 "FROM job_files, downloads " \
                                 "WHERE job_files.job_id=%d " \
                                    "AND job_files.file_id=downloads.id" % \
                                    job_submit['job_id'])
    for download in downloads:
        file = download['filename']
        if os.path.exists(file):
            os.remove(file)
            print "Deleted: %s" % file

