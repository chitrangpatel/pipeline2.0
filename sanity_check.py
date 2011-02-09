import config
import os
import stat
import sys
from PipelineQueueManager import PipelineQueueManager

def check_queuemanager():
    import job
    try:
        for name,func in job.PipelineQueueManager.__dict__.items():
            if name[:2] != "__":
                exec_string = "job.QueueManagerClass.%s(imp_test=True)" % name
                eval(exec_string)

    except NotImplementedError:
        print "!!!ERROR!!!\tYou must implement '%s' class method in you QueueManager class." % name
    
def main():
    read_write_directories = {
    'config.base_results_directory':config.base_results_directory , 
    'config.base_working_directory':config.base_working_directory ,
    'config.downloader_temp':config.downloader_temp,
    'config.bgs_db_file_path':config.bgs_db_file_path
    }
    for key,rw_dir in read_write_directories.items():
        if os.path.exists(rw_dir):
            if not (os.access(rw_dir,os.R_OK) and os.access(rw_dir,os.W_OK)):
                print "!!!ERROR!!!\tIt is possible that you don't have the right permissions for %s[%s]. Should be able to read/write" % (key,rw_dir)
        else:
            print "!!!ERROR!!!\t%s[%s] does not exist." % (key,rw_dir)


    path_to_check_for_existance_and_read_access = {
    'config.default_zaplist':config.default_zaplist,
    'config.zaplistdir':config.zaplistdir,
    'config.uploader_result_dir':config.uploader_result_dir
    }
    
    for key,ptcfe in path_to_check_for_existance_and_read_access.items():
        if os.path.exists(ptcfe):
            if not (os.access(ptcfe,os.R_OK)):
                print "!!!ERROR!!!\tIt is possible that you don't have the right permissions for %s[%s]. Should be able to read" % (key,ptcfe)                
        else:
            print "!!!ERROR!!!\t%s[%s] does not exist." % (key,ptcfe)

    check_queuemanager()

if __name__ == "__main__":
    main()