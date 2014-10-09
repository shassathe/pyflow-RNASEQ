import os
import os.path
import sys
import subprocess
import getpass
import glob
import ntpath

### Get path of current script and the main directory of pyflow to import WorkflowRunner form pyflow.py
filePath = os.path.abspath(os.path.dirname(__file__))
pyflowPath = os.path.abspath(os.path.join(filePath, "../src"))
sys.path.append(pyflowPath)


from pyflow import WorkflowRunner
from optparse import OptionParser

### Get options for the script such as directory containing the fastq files and the arguments for job submission to the GRID
parser = OptionParser()
parser.add_option("-i", "--input_dir", type="string", dest="indir",help="Directory name containing fastq files (Required)")
parser.add_option("-R", "--ref", type="string", dest="ref_org", default="hg19", help="Ref. organism. mm9|hg19")
parser.add_option("-j","--jobs",dest="jobs",default=15,help="maximum number of jobs to submit concurrently")
parser.add_option("-c","--cores",dest="cores",default=8,help="number of cores for star to use.")
parser.add_option("-o","--output_dir",dest="outdir",default=None,help="output directory")
parser.add_option("-l","--local",dest="local",action='store_true',default=False,help="run on cluster")
parser.add_option("-r","--restart",dest="restart",action="store_true",default=False,help="run on cluster")
parser.add_option("-q","--queue",dest="queue",default='hotel',help="run on cluster")

(options, args) = parser.parse_args()

if not os.path.exists(options.outdir):
        os.makedirs(options.dest)

class PyflowPipeline(WorkflowRunner):
        def __init__(self,files,output_directory,indir):
                self.input_directory = indir
                self.output_directory = output_directory
		self.f = files
			
	def workflow(self):
		print "building workflow...\nsubmitting jobs...\n"
		taskname = "fastqc"
		dependencies=()
		fastqc="fastqc -o %s %s"%(self.output_directory, files)
       		self.addTask(taskname,command=fastqc,dependencies=dependencies)

for dirpath, dirnames, filenames in os.walk(options.indir):
	for d in dirnames:
		dir = options.indir + d
		if os.path.isdir(dir):
			l = os.listdir(dir)
			l2=[]
			for f in l:
				if f.endswith('.fastq.gz'):
					fq = dir + "/" + f
					l2.append(fq)
			files=""
	                for i in l2:
	                        files+=i
        	                files+=" "
			pline = PyflowPipeline(files,options.outdir,options.indir)
                        mode = None
                        if options.local:
                                mode = "local"
                        else:
                                mode = "sge"
                        schedulerArgList = None
                        if options.queue != None:
                        	schedulerArgList = ["-q",options.queue]
                        pline.run(mode=mode,nCores=options.jobs,schedulerArgList=schedulerArgList,isContinue=options.restart)
