#!/usr/bin/env python

"""
A singlepulse candidate uploader for the PALFA survey.

Patrick Lazarus, May 15th, 2011
Paul Scholz May 11, 2015
"""
import os.path
import optparse
import glob
import types
import traceback
import sys
import datetime
import time
import tarfile
import tempfile
import shutil
import numpy as np

import debug
import CornellFTP
import database
import upload
import pipeline_utils

import config.basic
import config.upload

class SinglePulseTarball(upload.FTPable,upload.Uploadable):
    """A class to represent a tarball of single pulse files.
    """
    # A dictionary which contains variables to compare (as keys) and
    # how to compare them (as values)
    to_cmp = {'header_id': '%d', \
              'filename': '%s', \
              #'ftp_path': '%s', \
              'filetype': '%s', \
              'institution': '%s', \
              #'pipeline': '%s', \
              'pipeline': '%s'}
              #'versionnum': '%s'}
    
    def __init__(self, filename, versionnum, header_id=None, timestamp_mjd=None):
        self.header_id = header_id
        self.fullpath = filename
        self.filename = os.path.split(filename)[-1]
        self.versionnum = versionnum
        # Store a few configurations so the upload can be checked
        self.pipeline = config.basic.pipeline
        self.institution = config.basic.institution
        self.ftp_base = config.upload.sp_ftp_dir
        self.uploaded = False

        mjd = int(timestamp_mjd)
        self.ftp_path = os.path.join(self.ftp_base,str(mjd))

    def get_upload_sproc_call(self):
        """Return the EXEC spSinglePulseFileUpload string to upload
            this tarball's info to the PALFA common DB.
        """
        sprocstr = "EXEC spSinglePulseFileUpload " + \
            "@filename='%s', " % self.filename + \
            "@header_id=%d, " % self.header_id + \
            "@filetype='%s', " % self.filetype + \
            "@institution='%s', " % config.basic.institution + \
            "@pipeline='%s', " % config.basic.pipeline + \
            "@version_number='%s', " % self.versionnum + \
            "@file_location='%s', " % self.ftp_path + \
            "@uploaded=0"
        return sprocstr

    def check_already_uploaded(self, dbname='default'):
        """Check to see if already uploaded to DB (same as compare_with_db but
            returns True/False.

            Input:
                dbname: Name of database to connect to, or a database
                        connection to use (Defaut: 'default').
            Output:
                None
        """
        if isinstance(dbname, database.Database):
            db = dbname
        else:
            db = database.Database(dbname)
        db.execute("SELECT spf.header_id, " \
                        "spf.filename, " \
                        "spf.ftpfilepath as ftp_path," \
                        "spft.sp_files_type AS filetype, " \
                        "v.institution, " \
                        "v.pipeline " \
                        #"v.pipeline, " \
                        #"v.version_number AS versionnum " \
                  "FROM sp_files_info AS spf " \
                  "LEFT JOIN versions AS v ON v.version_id=spf.version_id " \
                  "LEFT JOIN sp_files_types AS spft " \
                        "ON spft.sp_files_type_id=spf.sp_files_type_id " \
                  #"WHERE spft.sp_files_type='%s' AND v.version_number='%s' AND " \
                  "WHERE spft.sp_files_type='%s' AND " \
                            "spf.header_id=%d AND v.institution='%s' AND " \
                            "v.pipeline='%s'" % \
                        #(self.filetype, self.versionnum, self.header_id, \
                        (self.filetype, self.header_id, \
                            config.basic.institution, config.basic.pipeline))
        rows = db.cursor.fetchall()
        if type(dbname) == types.StringType:
            db.close()
        if not rows:
            # No matching entry in common DB
            print "No matching entry in common DB"
            return False
        elif len(rows) > 1:
            # Too many matching entries!
            raise ValueError("Too many matching entries in common DB!\n" \
                                "(header_id: %d, filetype: %s, version_number: %s)" % \
                                (self.header_id, self.filetype, self.versionnum))
        else:
            desc = [d[0] for d in db.cursor.description]
            r = dict(zip(desc, rows[0]))
            errormsgs = []
            for var, fmt in self.to_cmp.iteritems():
                local = (fmt % getattr(self, var)).lower()
                fromdb = (fmt % r[var]).lower()
                if local != fromdb:
                    errormsgs.append("Values for '%s' don't match (local: %s, DB: %s)" % \
                                        (var, local, fromdb))
            if errormsgs:
                #Single pulse tarball info doesn't match returned row
                print "Single pulse tarball info doesn't match returned row"
                return False

            return True

    def compare_with_db(self, dbname='default'):
        """Grab corresponding file info from DB and compare values.
            Raise a SinglePulseCandidateError if any mismatch is found.

            Input:
                dbname: Name of database to connect to, or a database
                        connection to use (Defaut: 'default').
            Output:
                None
        """
        if isinstance(dbname, database.Database):
            db = dbname
        else:
            db = database.Database(dbname)
        db.execute("SELECT spf.header_id, " \
                        "spf.filename, " \
                        #"spf.ftpfilepath as ftp_path," \
                        "spft.sp_files_type AS filetype, " \
                        "v.institution, " \
                        "v.pipeline " \
                        #"v.pipeline, " \
                        #"v.version_number AS versionnum " \
                  "FROM sp_files_info AS spf " \
                  "LEFT JOIN versions AS v ON v.version_id=spf.version_id " \
                  "LEFT JOIN sp_files_types AS spft " \
                        "ON spft.sp_files_type_id=spf.sp_files_type_id " \
                  #"WHERE spft.sp_files_type='%s' AND v.version_number='%s' AND " \
                  "WHERE spft.sp_files_type='%s' AND " \
                            "spf.header_id=%d AND v.institution='%s' AND " \
                            "v.pipeline='%s'" % \
                        #(self.filetype, self.versionnum, self.header_id, \
                        (self.filetype, self.header_id, \
                            config.basic.institution, config.basic.pipeline))
        rows = db.cursor.fetchall()
        if type(dbname) == types.StringType:
            db.close()
        if not rows:
            # No matching entry in common DB
            raise ValueError("No matching entry in common DB!\n" \
                                "(header_id: %d, filetype: %s, version_number: %s)" % \
                                (self.header_id, self.filetype, self.versionnum))
        elif len(rows) > 1:
            # Too many matching entries!
            raise ValueError("Too many matching entries in common DB!\n" \
                                "(header_id: %d, filetype: %s, version_number: %s)" % \
                                (self.header_id, self.filetype, self.versionnum))
        else:
            desc = [d[0] for d in db.cursor.description]
            r = dict(zip(desc, rows[0]))
            errormsgs = []
            for var, fmt in self.to_cmp.iteritems():
                local = (fmt % getattr(self, var)).lower()
                fromdb = (fmt % r[var]).lower()
                if local != fromdb:
                    errormsgs.append("Values for '%s' don't match (local: %s, DB: %s)" % \
                                        (var, local, fromdb))
            if errormsgs:
                errormsg = "Single pulse tarball info doesn't match " \
                            "what was uploaded to the DB:"
                for msg in errormsgs:
                    errormsg += '\n    %s' % msg
                raise SinglePulseCandidateError(errormsg)

    def upload(self, dbname='default', *args, **kwargs):
        """And extension to the inherited 'upload' method.
            This method FTP's the file to Cornell instead of
            inserting it into the DB as a BLOB.

            Input:
                dbname: Name of database to connect to, or a database
                        connection to use (Defaut: 'default').
        """
        if self.header_id is None:
            raise SinglePulseCandidateError("Cannot upload SP tarball " \
                    "with header_id == None!")

        if self.check_already_uploaded(dbname=dbname):
            print self.filetype,"already uploaded. Will skip." 
            self.uploaded = True
        else:
            if debug.UPLOAD: 
                starttime = time.time()
            id = super(SinglePulseTarball, self).upload(dbname=dbname, \
                        *args, **kwargs)
            self.compare_with_db(dbname=dbname)
            
            if debug.UPLOAD:
                upload.upload_timing_summary['sp info (db)'] = \
                    upload.upload_timing_summary.setdefault('sp info (db)', 0) + \
                    (time.time()-starttime)
            if id < 0:
                # An error has occurred
                raise SinglePulseCandidateError(path)

    def upload_FTP(self,cftp,dbname='default'):
        """An extension to the inherited 'upload_FTP' method.
            This method FTP's the file to Cornell.

            Input:
                cftp: A CornellFTP connection.
        """
        if isinstance(dbname, database.Database):
            db = dbname
        else:
            db = database.Database(dbname)

        if debug.UPLOAD: 
                starttime = time.time()

        if not self.uploaded:

	    ftp_fullpath = os.path.join(self.ftp_path, self.filename)
            print self.ftp_path
            print cftp.dir_exists(self.ftp_path)
	    if not cftp.dir_exists(self.ftp_path):
		cftp.mkd(self.ftp_path)

            if ftp_fullpath in cftp.list_files(self.ftp_path):
                print "SP Tarball already there. Skipping FTP upload."
            else:
	        cftp.upload(self.fullpath, ftp_fullpath)

	    db.execute("spSPCandBinUploadConf " + \
		   "@sp_file_type='%s', " % self.filetype + \
		   "@filename='%s', " % self.filename + \
		   "@file_location='%s', " % self.ftp_path + \
		   "@uploaded=1" )
	    db.commit()

            self.uploaded = True

        if debug.UPLOAD:
            upload.upload_timing_summary['sp info (ftp)'] = \
                upload.upload_timing_summary.setdefault('sp info (ftp)', 0) + \
                (time.time()-starttime)


class SinglePulseCandsTarball(SinglePulseTarball):
    """A class to represent a tarball of *.singlepulse files.
    """
    filetype = "PRESTO singlepulse candidate tarball"


class SinglePulseInfTarball(SinglePulseTarball):
    """A class to represent a tarball of the *.inf files 
        required by *.singlepulse files.
    """
    filetype = "PRESTO singlepulse inf tarball"


class SinglePulseBeamPlot(upload.Uploadable):
    """A class to represent a per-beam single pulse plot.
    """
    # A dictionary which contains variables to compare (as keys) and
    # how to compare them (as values)
    to_cmp = {'header_id': '%d', \
              'filename': '%s', \
              'sp_plot_type': '%s', \
              'institution': '%s', \
              'pipeline': '%s', \
              'versionnum': '%s', \
              'datalen': '%d'}
    
    def __init__(self, plotfn, versionnum, header_id=None):
        self. header_id = header_id
        self.versionnum = versionnum
        self.filename = os.path.split(plotfn)[-1]
        self.datalen = os.path.getsize(plotfn)
        plot = open(plotfn, 'r')
        self.filedata = plot.read()
        plot.close()
        
        # Store a few configurations so the upload can be checked
        self.pipeline = config.basic.pipeline
        self.institution = config.basic.institution

    def upload(self, dbname, *args, **kwargs):
        """An extension to the inherited 'upload' method.

            Input:
                dbname: Name of database to connect to, or a database
                        connection to use (Defaut: 'default').
        """
        if self.header_id is None:
            raise SinglePulseCandidateError("Cannot upload SP plot " \
                    "with header_id == None!")
        if debug.UPLOAD: 
            starttime = time.time()
        super(SinglePulseBeamPlot, self).upload(dbname=dbname, \
                *args, **kwargs)
        self.compare_with_db(dbname=dbname)
        
        if debug.UPLOAD:
            upload.upload_timing_summary['sp plots'] = \
                upload.upload_timing_summary.setdefault('sp plots', 0) + \
                (time.time()-starttime)

    def get_upload_sproc_call(self):
        """Return the EXEC spSPSingleBeamCandPlotLoader string to
            upload this singlepulse plot to the PALFA common DB.
        """
        sprocstr = "EXEC spSPSingleBeamCandPlotLoader " + \
            "@header_id='%d', " % self.header_id + \
            "@sp_plot_type='%s', " % self.sp_plot_type + \
            "@filename='%s', " % os.path.split(self.filename)[-1] + \
            "@filedata=0x%s, " % self.filedata.encode('hex') + \
            "@institution='%s', " % config.basic.institution + \
            "@pipeline='%s', " % config.basic.pipeline + \
            "@version_number='%s'" % self.versionnum
        return sprocstr

    def compare_with_db(self, dbname='default'):
        """Grab corresponding singlepulse plot from DB and compare values.
            Raise a SinglePulseCandidateError if any mismatch is found.

            Input:
                dbname: Name of database to connect to, or a database
                        connection to use (Defaut: 'default').
            Output:
                None
        """
        if isinstance(dbname, database.Database):
            db = dbname
        else:
            db = database.Database(dbname)
        db.execute("SELECT spsb.header_id, " \
                        "spsbtype.sp_single_beam_plot_type AS sp_plot_type, " \
                        "spsb.filename, " \
                        "DATALENGTH(spsb.filedata) AS datalen, " \
                        "v.institution, " \
                        "v.pipeline, " \
                        "v.version_number AS versionnum " \
                    "FROM sp_plots_single_beam AS spsb " \
                    "LEFT JOIN versions AS v on v.version_id=spsb.version_id " \
                    "LEFT JOIN sp_single_beam_plot_types AS spsbtype " \
                        "ON spsb.sp_single_beam_plot_type_id=spsbtype.sp_single_beam_plot_type_id " \
                    "WHERE spsb.header_id=%d AND v.version_number='%s' AND " \
                            "v.institution='%s' AND v.pipeline='%s' AND " \
                            "spsbtype.sp_single_beam_plot_type='%s'" % \
                        (self.header_id, self.versionnum, \
                            config.basic.institution, config.basic.pipeline, \
                            self.sp_plot_type))
        rows = db.cursor.fetchall()
        if type(dbname) == types.StringType:
            db.close()
        if not rows:
            # No matching entry in common DB
            raise ValueError("No matching entry in common DB!\n" \
                                "(header_id: %d, sp_plot_type: %s, version_number: %s)" % \
                                (self.header_id, self.sp_plot_type, self.versionnum))
        elif len(rows) > 1:
            # Too many matching entries!
            raise ValueError("Too many matching entries in common DB!\n" \
                                "(header_id: %d, sp_plot_type: %s, version_number: %s)" % \
                                (self.header_id, self.sp_plot_type, self.versionnum))
        else:
            desc = [d[0] for d in db.cursor.description]
            r = dict(zip(desc, rows[0]))
            errormsgs = []
            for var, fmt in self.to_cmp.iteritems():
                local = (fmt % getattr(self, var)).lower()
                fromdb = (fmt % r[var]).lower()
                if local != fromdb:
                    errormsgs.append("Values for '%s' don't match (local: %s, DB: %s)" % \
                                        (var, local, fromdb))
            if errormsgs:
                errormsg = "Single pulse tarball info doesn't match " \
                            "what was uploaded to the DB:"
                for msg in errormsgs:
                    errormsg += '\n    %s' % msg
                raise SinglePulseCandidateError(errormsg)


class SinglePulseBeamPlotDMs0_110(SinglePulseBeamPlot):
    """A class to represent a PRESTO per-beam singlepulse plot for
        DMs 0 to 110.
    """
    sp_plot_type = "PRESTO singlepulse per-beam plot (DMs 0 to 110)"


class SinglePulseBeamPlotDMs100_310(SinglePulseBeamPlot):
    """A class to represent a PRESTO per-beam singlepulse plot for
        DMs 100 to 310.
    """
    sp_plot_type = "PRESTO singlepulse per-beam plot (DMs 100 to 310)"


class SinglePulseBeamPlotDMs300AndUp(SinglePulseBeamPlot):
    """A class to represent a PRESTO per-beam singlepulse plot for
        DMs 300 and up.
    """
    sp_plot_type = "PRESTO singlepulse per-beam plot (DMs 300 and up)"

class SinglePulseCandidate(upload.Uploadable,upload.FTPable):
    """A class to represent a PALFA single pulse candidate.
    """
    # A dictionary which contains variables to compare (as keys) and
    # how to compare them (as values)
    to_cmp = {'header_id': '%d', \
              'cand_num': '%d', \
              'time': '%.12g', \
              'dm': '%.12g', \
              'delta_dm': '%.12g', \
              'width': '%.12g', \
              'snr': '%.12g', \
              'num_hits': '%d', \
              'institution': '%s', \
              'pipeline': '%s', \
              'versionnum': '%s', \
              'sifting_code': '%s', \
              'sifting_fom': '%.12g'}

    def __init__(self, cand_num, spd, versionnum, \
                        header_id=None):
        self.header_id = header_id # Header ID from database
        self.cand_num = cand_num # Unique identifier of candidate within beam's 
                                 # list of candidates; Candidate's position in
                                 # a list of all candidates produced in beam
                                 # ordered by decreasing sigma (where largest
                                 # sigma has cand_num=1).
        self.time = spd.pulse_peak_time # Time in timeseries of pulse
        self.dm = spd.bestdm # Dispersion measure
        self.delta_dm = np.max(spd.dmVt_this_dms) - np.min(spd.dmVt_this_dms) # DM span of group
        self.width = spd.pulsewidth_seconds # width of boxcar that discovered event
        self.snr = spd.sigma # signal-to-noise ratio, singlepulse sigma of peak event in group
        self.num_hits = len(spd.dmVt_this_dms) # Number of singlepulse events in group
        self.versionnum = versionnum # Version number; a combination of PRESTO's githash
                                     # and pipeline's githash

        # Sifting code used and its figure-of-merit
        self.sifting_code = "RRATtrap" # un-hardcode?
        self.sifting_fom = spd.rank

        # Store a few configurations so the upload can be checked
        self.pipeline = config.basic.pipeline
        self.institution = config.basic.institution
    
        # List of dependents (ie other uploadables that require 
        # the sp_cand_id from this candidate)
        self.dependents = []

    def add_dependent(self, dep):
        self.dependents.append(dep)

    def upload(self, dbname, *args, **kwargs):
        """An extension to the inherited 'upload' method.
            This method will make sure any dependents have
            the sp_cand_id and then upload them.

            Input:
                dbname: Name of database to connect to, or a database
                        connection to use (Defaut: 'default').
        """
        if self.header_id is None:
            raise SinglePulseCandidateError("Cannot upload candidate with " \
                    "header_id == None!")
        if debug.UPLOAD: 
            starttime = time.time()
        sp_cand_id = super(SinglePulseCandidate, self).upload(dbname=dbname, \
                      *args, **kwargs)[0]
        
        self.compare_with_db(dbname=dbname)

        if debug.UPLOAD:
            upload.upload_timing_summary['sp candidates'] = \
                upload.upload_timing_summary.setdefault('sp candidates', 0) + \
                (time.time()-starttime)
        for dep in self.dependents:
            dep.sp_cand_id = sp_cand_id
            dep.upload(dbname=dbname, *args, **kwargs)
        return sp_cand_id

    def upload_FTP(self, cftp, dbname):
        for dep in self.dependents:
           if isinstance(dep,upload.FTPable):
               dep.upload_FTP(cftp,dbname=dbname)

    def get_upload_sproc_call(self):
        """Return the EXEC spPDMCandUploaderFindsVersion string to upload
            this candidate to the PALFA common DB.
        """
        sprocstr = "EXEC spSPCandUploaderFindsVersion " + \
            "@header_id=%d, " % self.header_id + \
            "@cand_num=%d, " % self.cand_num + \
            "@time=%.12g, " % self.time + \
            "@dm=%.12g, " % self.dm + \
            "@delta_dm=%.12g, " % self.delta_dm + \
            "@width=%.12g, " % self.width + \
            "@snr=%.12g, " % self.snr + \
            "@num_hits=%d, " % self.num_hits + \
            "@institution='%s', " % config.basic.institution + \
            "@pipeline='%s', " % config.basic.pipeline + \
            "@version_number='%s', " % self.versionnum + \
            "@proc_date='%s', " % datetime.date.today().strftime("%Y-%m-%d") + \
            "@sifting_code='%s', " % self.sifting_code + \
            "@sifting_fom=%.12g" %self.sifting_fom
        return sprocstr

    def compare_with_db(self, dbname='default'):
        """Grab corresponding candidate from DB and compare values.
            Raise a SinglePulseCandidateError if any mismatch is found.

            Input:
                dbname: Name of database to connect to, or a database
                        connection to use (Defaut: 'default').
            Outputs:
                None
        """
        if isinstance(dbname, database.Database):
            db = dbname
        else:
            db = database.Database(dbname)
        db.execute("SELECT c.header_id, " \
                        "c.cand_num, " \
                        "c.time, " \
                        "c.dm, " \
                        "c.delta_dm, " \
                        "c.width, " \
                        "c.snr, " \
                        "c.num_hits, " \
                        "v.institution, " \
                        "v.pipeline, " \
                        "v.version_number AS versionnum, " \
                        "c.sifting_code, " \
                        "c.sifting_fom " \
                  "FROM sp_candidates AS c " \
                  "LEFT JOIN versions AS v ON v.version_id=c.version_id " \
                  "WHERE c.cand_num=%d AND v.version_number='%s' AND " \
                            "c.header_id=%d " % \
                        (self.cand_num, self.versionnum, self.header_id))
        rows = db.cursor.fetchall()
        if type(dbname) == types.StringType:
            db.close()
        if not rows:
            # No matching entry in common DB
            raise ValueError("No matching entry in common DB!\n" \
                                "(header_id: %d, cand_num: %d, version_number: %s)" % \
                                (self.header_id, self.cand_num, self.versionnum))
        elif len(rows) > 1:
            # Too many matching entries!
            raise ValueError("Too many matching entries in common DB!\n" \
                                "(header_id: %d, cand_num: %d, version_number: %s)" % \
                                (self.header_id, self.cand_num, self.versionnum))
        else:
            desc = [d[0] for d in db.cursor.description]
            r = dict(zip(desc, rows[0]))
            errormsgs = []
            for var, fmt in self.to_cmp.iteritems():
                local = (fmt % getattr(self, var)).lower()
                fromdb = (fmt % r[var]).lower()
                if local != fromdb:
                    errormsgs.append("Values for '%s' don't match (local: %s, DB: %s)" % \
                                        (var, local, fromdb))
            if errormsgs:
                errormsg = "SP Candidate doesn't match what was uploaded to the DB:"
                for msg in errormsgs:
                    errormsg += '\n    %s' % msg
                raise SinglePulseCandidateError(errormsg)

class SinglePulseCandidatePlot(upload.Uploadable):
    """A class to represent the plot of a PALFA periodicity candidate.
    """
    # A dictionary which contains variables to compare (as keys) and
    # how to compare them (as values)
    to_cmp = {'cand_id': '%d', \
              'plot_type': '%s', \
              'filename': '%s', \
              'datalen': '%d'}
    
    def __init__(self, plotfn, sp_cand_id=None):
        self.sp_cand_id = sp_cand_id
        self.filename = os.path.split(plotfn)[-1]
        self.datalen = os.path.getsize(plotfn)
        plot = open(plotfn, 'r')
        self.filedata = plot.read()
        plot.close()

    def upload(self, dbname, *args, **kwargs):
        """An extension to the inherited 'upload' method.

            Input:
                dbname: Name of database to connect to, or a database
                        connection to use (Defaut: 'default').
        """
        if self.sp_cand_id is None:
            raise SinglePulseCandidateError("Cannot upload plot with " \
                    "sp_cand_id == None!")
        if debug.UPLOAD: 
            starttime = time.time()
        super(SinglePulseCandidatePlot, self).upload(dbname=dbname, \
                    *args, **kwargs)
        self.compare_with_db(dbname=dbname)
        
        if debug.UPLOAD:
            upload.upload_timing_summary[self.plot_type] = \
                upload.upload_timing_summary.setdefault(self.plot_type, 0) + \
                (time.time()-starttime)

    def get_upload_sproc_call(self):
        """Return the EXEC spPDMCandPlotUploader string to upload
            this candidate plot to the PALFA common DB.
        """
        sprocstr = "EXEC spPDMCandPlotLoader " + \
            "@sp_cand_id=%d, " % self.sp_cand_id + \
            "@sp_plot_type='%s', " % self.plot_type + \
            "@filename='%s', " % os.path.split(self.filename)[-1] + \
            "@filedata=0x%s" % self.filedata.encode('hex')
        return sprocstr

    def compare_with_db(self, dbname='default'):
        """Grab corresponding candidate plot from DB and compare values.
            Raise a SinglePulseCandidateError if any mismatch is found.
            
            Input:
                dbname: Name of database to connect to, or a database
                        connection to use (Defaut: 'default').
            Output:
                None
        """
        if isinstance(dbname, database.Database):
            db = dbname
        else:
            db = database.Database(dbname)
        db.execute("SELECT plt.sp_cand_id AS sp_cand_id, " \
                        "pltype.sp_plot_type AS plot_type, " \
                        "plt.filename, " \
                        "DATALENGTH(plt.filedata) AS datalen " \
                   "FROM sp_candidate_plots AS plt " \
                   "LEFT JOIN sp_plot_types AS pltype " \
                        "ON plt.sp_plot_type_id=pltype.sp_plot_type_id " \
                   "WHERE plt.sp_cand_id=%d AND pltype.sp_plot_type='%s' " % \
                        (self.cand_id, self.plot_type))
        rows = db.cursor.fetchall()
        if type(dbname) == types.StringType:
            db.close()
        if not rows:
            # No matching entry in common DB
            raise ValueError("No matching entry in common DB!\n" \
                                "(sp_cand_id: %d, sp_plot_type: %s)" % \
                                (self.cand_id, self.plot_type))
        elif len(rows) > 1:
            # Too many matching entries!
            raise ValueError("Too many matching entries in common DB!\n" \
                                "(sp_cand_id: %d, sp_plot_type: %s)" % \
                                (self.cand_id, self.plot_type))
        else:
            desc = [d[0] for d in db.cursor.description]
            r = dict(zip(desc, rows[0]))
            errormsgs = []
            for var, fmt in self.to_cmp.iteritems():
                local = (fmt % getattr(self, var)).lower()
                fromdb = (fmt % r[var]).lower()
                if local != fromdb:
                    errormsgs.append("Values for '%s' don't match (local: %s, DB: %s)" % \
                                        (var, local, fromdb))
            if errormsgs:
                errormsg = "SP Candidate plot doesn't match what was uploaded to the DB:"
                for msg in errormsgs:
                    errormsg += '\n    %s' % msg
                raise SinglePulseCandidateError(errormsg)


class SinglePulseCandidatePNG(SinglePulseCandidatePlot):
    """A class to represent single pulse (spd) candidate PNGs.
    """
    plot_type = "spd plot"

class SinglePulseCandidateBinary(upload.FTPable,upload.Uploadable):
    """A class to represent a single pulse candidate binary that
       needs to be FTPed to Cornell.
    """
    # A dictionary which contains variables to compare (as keys) and
    # how to compare them (as values)
    to_cmp = {'sp_cand_id': '%d', \
              'filetype': '%s', \
              'filename': '%s'}
    
    def __init__(self, filename, filesize, sp_cand_id=None, remote_spd_dir=None):
        self.sp_cand_id = cand_id
        self.fullpath = filename 
        self.filename = os.path.split(filename)[-1]
        self.filesize = filesize
        self.ftp_base = config.upload.spd_ftp_dir
        self.uploaded = False

        self.ftp_path = remote_spd_dir

    def get_upload_sproc_call(self):
        """Return the EXEC spPFDBLAH string to upload
            this binary's info to the PALFA common DB.
        """
        sprocstr = "EXEC spPDMCandBinFSLoader " + \
            "@sp_cand_id=%d, " % self.sp_cand_id + \
            "@sp_plot_type='%s', " % self.filetype + \
            "@filename='%s', " % self.filename + \
            "@file_location='%s', " % self.ftp_path + \
            "@uploaded=0 "

        return sprocstr

    def compare_with_db(self,dbname='default'):
        """Grab corresponding file info from DB and compare values.
            Raise a SinglePulseCandidateError if any mismatch is found.

            Input:
                dbname: Name of database to connect to, or a database
                        connection to use (Defaut: 'default').
            Output:
                None
        """
        if isinstance(dbname, database.Database):
            db = dbname
        else:
            db = database.Database(dbname)
        db.execute("SELECT bin.sp_cand_id AS sp_cand_id, " \
                        "pltype.sp_plot_type AS filetype, " \
                        "bin.filename, " \
                        "bin.file_location AS ftp_path " \
                   "FROM SP_Candidate_Binaries_Filesystem AS bin " \
                   "LEFT JOIN sp_plot_types AS pltype " \
                        "ON bin.sp_plot_type_id=pltype.sp_plot_type_id " \
                   "WHERE bin.sp_cand_id=%d AND pltype.sp_plot_type='%s' " % \
                        (self.sp_cand_id, self.filetype))
        rows = db.cursor.fetchall()
        if type(dbname) == types.StringType:
            db.close()
        if not rows:
            # No matching entry in common DB
            raise ValueError("No matching entry in common DB!\n" \
                                "(sp_cand_id: %d, filetype: %s)" % \
                                (self.sp_cand_id, self.filetype))
        elif len(rows) > 1:
            # Too many matching entries!
            raise ValueError("Too many matching entries in common DB!\n" \
                                "(sp_cand_id: %d, filetype: %s)" % \
                                (self.sp_cand_id, self.filetype))
        else:
            desc = [d[0] for d in db.cursor.description]
            r = dict(zip(desc, rows[0]))
            errormsgs = []
            for var, fmt in self.to_cmp.iteritems():
                local = (fmt % getattr(self, var)).lower()
                fromdb = (fmt % r[var]).lower()
                if local != fromdb:
                    errormsgs.append("Values for '%s' don't match (local: %s, DB: %s)" % \
                                        (var, local, fromdb))
            if errormsgs:
                errormsg = "SP Candidate binary info doesn't match what was uploaded to the DB:"
                for msg in errormsgs:
                    errormsg += '\n    %s' % msg
                raise SinglePulseCandidateError(errormsg)

    def upload(self, dbname, *args, **kwargs):
        """An extension to the inherited 'upload' method.

            Input:
                dbname: Name of database to connect to, or a database
                        connection to use (Defaut: 'default').
        """
        if self.sp_cand_id is None:
            raise SinglePulseCandidateError("Cannot upload binary with " \
                    "sp_cand_id == None!")

        if debug.UPLOAD: 
            starttime = time.time()
        super(SinglePulseCandidateBinary, self).upload(dbname=dbname, \
                         *args, **kwargs)
        self.compare_with_db(dbname=dbname)
        
        if debug.UPLOAD:
            upload.upload_timing_summary[self.filetype + ' (db)'] = \
                upload.upload_timing_summary.setdefault(self.filetype + ' (db)', 0) + \
                (time.time()-starttime)

    def upload_FTP(self, cftp, dbname='default'): 
        """An extension to the inherited 'upload_FTP' method.
            This method checks that the binary file was 
            successfully uploaded to Cornell.

            Input:
                cftp: A CornellFTP connection.
        """
        if isinstance(dbname, database.Database):
            db = dbname
        else:
            db = database.Database(dbname)

        if debug.UPLOAD: 
            starttime = time.time()

        if not self.uploaded:

	    ftp_fullpath = os.path.join(self.ftp_path, self.filename) 
	    #if cftp.dir_exists(self.ftp_path):
	    remotesize = cftp.get_size(ftp_fullpath)	
            #else:
            #remotesize = -1

            if remotesize == self.filesize:
	        db.execute("EXEC spSPCandBinUploadConf " + \
	               "@sp_plot_type='%s', " % self.filetype + \
	               "@filename='%s', " % self.filename + \
	               "@file_location='%s', " % self.ftp_path + \
	               "@uploaded=1") 
	        db.commit() 

	        self.uploaded=True
            else:
                errormsg = "Size of binary file %s on remote server does not match local"\
                           " size.\n\tRemote size (%d bytes) != Local size (%d bytes)" % \
                           (self.filename,remote_size,self.filesize)
                raise SinglePulseCandidateError(errormsg)

        if debug.UPLOAD:
            upload.upload_timing_summary[self.filetype + ' (ftp-check)'] = \
                upload.upload_timing_summary.setdefault(self.filetype + ' (ftp-check)', 0) + \
                (time.time()-starttime)
        

class SinglePulseCandidateSPD(SinglePulseCandidateBinary):
    """A class to represent single-pulse candidate SPD files.
    """
    filetype = "spd binary"


class SPDTarball(upload.FTPable):
    """ Extract the tarball of spd files (will we have them tarred up?) and
        upload them to the Cornell FTP server """

    def __init__(self,tarfn,remote_dir,tempdir):
        basename = os.path.basename(tarfn).rstrip('_spd.tgz')

        self.local_spd_dir = os.path.join(tempdir,basename)
        self.remote_dir = remote_dir
        self.tempdir = tempdir
        self.tarfn = tarfn

    def extract(self):
        os.mkdir(self.local_spd_dir)

        #extract spd tarball to temporary dir
        tar = tarfile.open(self.tarfn)
        try:
            tar.extractall(path=self.local_spd_dir)
        except IOError:
            if os.path.isdir(self.tempdir):
                shutil.rmtree(self.tempdir)
            raise PeriodicityCandidateError("Error while extracting spd files " \
                                            "from tarball (%s)!" % tarfn)
        finally:
            tar.close()

        files = glob.glob(os.path.join(self.local_spd_dir,'*.spd'))
        sizes = [os.path.getsize(os.path.join(self.local_spd_dir, fn)) for fn in files]

        return self.local_spd_dir,zip(files,sizes)

    def upload_FTP(self,cftp,dbname='default'):

        # upload the spds to Cornell using the lftp mirror command
        if debug.UPLOAD: 
            starttime = time.time()

        try:
            CornellFTP.mirror(self.local_spd_dir,self.remote_dir,reverse=True,parallel=10)
        except:
            raise

        if debug.UPLOAD:
            upload.upload_timing_summary['spd (ftp)'] = \
                upload.upload_timing_summary.setdefault('spd (ftp)', 0) + \
                (time.time()-starttime)

class SinglePulseCandidateError(upload.UploadNonFatalError):
    """Error to throw when a single pulse candidate-specific problem is encountered.
    """
    pass


def get_spcandidates(versionnum, directory, header_id=None, timestamp_mjd=None):
    """Return single pulse candidates to common DB.

        Inputs:
            versionnum: A combination of the githash values from 
                        PRESTO, the pipeline, and psrfits_utils.
            directory: The directory containing results from the pipeline.
            header_id: header_id number for this beam, as returned by
                        spHeaderLoader/header.upload_header

        Ouputs:
            sp_cands: List of single pulse candidates, plots and tarballs.
    """
    sp_cands = []

    # Create temporary directory
    tempdir = tempfile.mkdtemp(suffix="_tmp", prefix="PALFA_spds_")

    mjd = int(timestamp_mjd)
    remote_spd_base = os.path.join(config.upload.spd_ftp_dir,str(mjd)) 
    remote_spd_dir = os.path.join(remote_spd_base,\
                                  os.path.basename(spd_tarfns[0]).rstrip('_spd.tgz'))

    # extract spd tarball
    spd_tarfns = glob.glob(os.path.join(directory, "*_spd.tgz"))
    spd_tarball = SPDTarball(spd_tarfns[0],remote_spd_base,tempdir)
    spd_tempdir, spd_list = spd_tarball.extract()

    # extract ratings # TODO 

    # Gather SP candidates and their rating and plots
    sp_cands.append(spd_tarball)
    for ii,spd_elem in enumerate(spd_list):
        spdfn, spd_size = spd_elem[0], spd_elem[1]
        pngfn = spdfn.replace(".spd",".spd.png")
        ratfn = spdfn.replace(".spd",".spd.rat")

        spd = sp_utils.spd(spdfn)
        cand = SinglePulseCandidate(ii+1, spd, versionnum, header_id=header_id)
        cand.add_dependent(SinglePulseCandidatePNG(pngfn))
        cand.add_dependent(SinglePulseCandidateSPD(spdfn, spd_size, remote_spd_dir=remote_spd_dir))

        ratvals = ratings2.rating_value.read_file(ratfn)
        cand.add_dependent(SinglePulseCandidateRating(ratvals,inst_cache=inst_cache))

        sp_cands.append(cand)

    # Gather per-beam plots to upload
    fns = glob.glob(os.path.join(directory, "*DMs0-110_singlepulse.png"))
    if len(fns) != 1:
        raise SinglePulseCandidateError("Wrong number of *DMs0-110_singlepulse.png " \
                                        "plots found (%d)!" % len(fns))
    sp_cands.append(SinglePulseBeamPlotDMs0_110(fns[0], versionnum, \
                        header_id=header_id))

    fns = glob.glob(os.path.join(directory, "*DMs100-310_singlepulse.png"))
    if len(fns) != 1:
        raise SinglePulseCandidateError("Wrong number of *DMs100-310_singlepulse.png " \
                                        "plots found (%d)!" % len(fns))
    sp_cands.append(SinglePulseBeamPlotDMs100_310(fns[0], versionnum, \
                        header_id=header_id))

    fns = glob.glob(os.path.join(directory, "*DMs300-1000+_singlepulse.png"))
    if len(fns) != 1:
        raise SinglePulseCandidateError("Wrong number of *DMs300-1000_singlepulse.png " \
                                        "plots found (%d)!" % len(fns))
    sp_cands.append(SinglePulseBeamPlotDMs300AndUp(fns[0], versionnum, \
                        header_id=header_id))
    
    # Gather per-beam SP tarballs to upload
    fns = glob.glob(os.path.join(directory, "*_inf.tgz"))
    if len(fns) != 1:
        raise SinglePulseCandidateError("Wrong number of *_inf.tgz " \
                                        "tarballs found (%d)!" % len(fns))
    sp_cands.append(SinglePulseInfTarball(fns[0], versionnum, \
                        header_id=header_id, timestamp_mjd=timestamp_mjd))
    
    fns = glob.glob(os.path.join(directory, "*_singlepulse.tgz"))
    if len(fns) != 1:
        raise SinglePulseCandidateError("Wrong number of *_singlepulse.tgz " \
                                        "tarballs found (%d)!" % len(fns))
    sp_cands.append(SinglePulseCandsTarball(fns[0] , versionnum, \
                        header_id=header_id, timestamp_mjd=timestamp_mjd))

    return sp_cands, tempdir

