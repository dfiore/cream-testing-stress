#!/usr/bin/python2.6

# Copyright 2012 Dimosthenes Fioretos dfiore -at- noc -dot- uoa -dot- gr
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

#########################################################################################################
#                                       NOTES                                                           #
#-------------------------------------------------------------------------------------------------------#
# Version       |   1.0                                                                                 #
#-------------------------------------------------------------------------------------------------------#
# Dependencies  |   python-argparse                                                                     #
#-------------------------------------------------------------------------------------------------------#
# Invocation    |   Run script with -h argument                                                         #
#-------------------------------------------------------------------------------------------------------#
#    Notes      |   1) A very big -c argument will lead to a system lock-up, due to memory shortage     #
#               |   2) If the slots argument is very big -and especially if the submitted jobs are      #
#               |      short lived-, jobs might be left unaccounted until a process has filled its      #
#               |      own slots.                                                                       #
#########################################################################################################

from multiprocessing import Process, Lock, Value, Array, Queue, queues
import multiprocessing
import time, sys, random, argparse, re, datetime, subprocess , shlex, string, tempfile, signal, traceback, os
#import pycallgraph


class _error(Exception):
    def __init__(self,string):
        self.string = string
    def __str__(self):
        return str(self.string)
###############################################################################################################
###############################################################################################################
#class sync_queue(_queue.Queue):
class sync_queue(multiprocessing.queues.Queue):
        '''
                A multiprocessing.queues.Queue object with a lock implemented to sync its operations.
                Supports the add() and remove() operations.
        '''

        def __init__(self):
                self.q = Queue()
                self.lock = Lock()

        def add(self, item):
                #with self.lock:                #"with" supports lock objects automagically
                        #self.q.put(item)       # dropped it for traditional acquire()/release(), for readability mainly.

                self.lock.acquire()
                #print 'Putting in queue: ' + str(item)
                self.q.put(item)
                self.lock.release()

        def remove(self):
                return self.q.get()
###############################################################################################################
###############################################################################################################
class sync_print:
        '''
                A class performing pretty printing with some convinience methods and synced access to the terminal
                through a shared lock. Methods:
                disable_color()         :       disables color
                enable_color()          :       enables color (on by default)
                disable_print()         :       disable printing
                enable_print()          :       enable printing (on by default)
                safe_print()            :       print to the screen in a synced manner, with pretty colors and timestamps
        '''

        YELLOW='\033[1;33m'
        RED='\033[1;31m'
        MAGENTA='\033[1;35m'
        BLUE='\033[1;34m'
        BLACK='\033[0;30m'
        GREY='\033[1;30m'
        END='\033[0m'
        disable=False

        def __init__(self):
                self.lock = Lock()

        def disable_color(self):
                self.YELLOW='' ; self.RED='' ; self.MAGENTA='' ; self.BLUE=''
                self.BLACK='' ; self.GREY='' ; self.END=''

        def enable_color(self):
                self.YELLOW='\033[1;33m' ; self.RED='\033[1;31m' ; self.MAGENTA='\033[1;35m' 
                self.BLUE='\033[1;34m' ; self.BLACK='\033[0;30m' ; self.GREY='\033[1;30m'
                self.END='\033[0m'

        def disable_print(self):
                self.disable=True

        def enable_print(self):
                self.disable=False

        def safe_print(self, message, color_type=None):
                if self.disable == True:
                        return

                self.lock.acquire()
                timestamp = datetime.datetime.now()

                if color_type is None or color_type not in ['yellow','red','magenta','blue','black','grey'] :
                        print str(timestamp) + '\t' + message
                else:
                        if color_type == 'yellow':
                                cCode=self.YELLOW
                        elif color_type == 'red':
                                cCode=self.RED
                        elif color_type == 'magenta':
                                cCode=self.MAGENTA
                        elif color_type == 'blue':
                                cCode=self.BLUE
                        elif color_type == 'black':
                                cCode=self.BLACK
                        elif color_type == 'grey':
                                cCode=self.GREY

                        print str(timestamp) + '\t' + cCode + message + self.END

                self.lock.release()
###############################################################################################################
###############################################################################################################
def f(vLock, sp, my_id, jobs_left, jdl_path, endpoint, deleg_id, output_dir, testType, q, slots, start, duration):
        '''
                submitter function
        '''

        final_states = ['DONE-OK', 'DONE-FAILED', 'ABORTED', 'CANCELLED']
        jids = []
        my_pid = str(os.getpid())
        q.add(['pid',my_pid]) #send manager my pid

        #pycallgraph.start_trace()

        while True:
                vLock.acquire()
                if jobs_left.value == 0 and len(jids) == 0: #no jobs left to submit or collect
                        vLock.release()
                        sp.safe_print('Process ' + my_pid + ' terminating, no jobs left to submit or collect', 'grey')
                        #pycallgraph.make_dot_graph('proc'+my_pid+'.png')
                        sys.exit(0)
                else: #jobs are left to be submitted or to be collected
                        vLock.release()

                        if len(jids) < slots: #if must submit jobs
                                while len(jids) < slots: #submit new jobs only if there are self-slots available
                                        vLock.acquire()
                                        if jobs_left.value == 0: #if jobs left to submit
                                                vLock.release()
                                                break
                                        else:
                                                now = datetime.datetime.now()

                                                if duration == 0:
                                                        jobs_left.value = jobs_left.value-1
                                                        my_jobs_left = jobs_left.value
                                                else:
                                                        my_jobs_left = jobs_left.value
                                                        elapsed = now - start
                                                        #sp.safe_print('Process ' + my_pid + ': living '+str(elapsed.seconds)+'s ecs', 'red')
                                                        if elapsed.seconds >= duration*60:
                                                                jobs_left.value = 0
                                                                vLock.release()
                                                                break

                                                vLock.release()

                                        (jid, retVal) = submit_wrapper(jdl_path, endpoint, deleg_id, sp, 
                                                                       q, jobs_left.value, my_jobs_left,
                                                                       start, duration, now)

                                        if retVal == 'fail':
                                                continue
                                        else:
                                                jids.append(jid)

                                        if testType == "cancel": #if test type is cancel, perform the operation
                                                (retVal) = cancel_wrapper(jid, sp, q)
                        #else: #don't have to submit jobs, so collect them
                        for item in jids: #collect jobs
                                (status, retVal) = job_status_wrapper(item, sp, q, output_dir)

                                if status in final_states or retVal == 'fail': #if job finished or cannot be found, log it and remove it
                                        if 'DONE-OK' == status:
                                                q.add([item,'done_ok'])
                                        elif 'DONE-FAILED' == status:
                                                q.add([item,'failed'])
                                        elif 'ABORTED' == status:
                                                q.add([item,'aborted'])
                                        elif 'CANCELLED' == status:
                                                q.add([item,'cancelled'])
                                        else:
                                                q.add([item,'illegal'])

                                        jids.remove(item)
###############################################################################################################
###############################################################################################################
def g(q, sp, numJobs, start, duration):
        '''
                manager function
        '''

        submitted     = []
        running       = []
        cancelled     = []
        done_ok       = []
        failed        = []
        aborted       = []
        stat_failed   = []
        submit_failed = []
        cancel_failed = []
        illegal       = []
        pids          = []

        while True:
                arr = q.remove()

                if arr[0] == 'pid':
                        pids.append(arr[1])
                        continue
                else:
                        if arr[1] == 'not-set':
                                pass #initial value, ignore it
                        elif arr[1] == 'submitted':
                                submitted.append(arr[0])
                                running.append(arr[0])  # running accumulates the submitted and not yet collected(finished) jobs
                        elif arr[1] == 'cancelled':
                                cancelled.append(arr[0])
                        elif arr[1] == 'done_ok':
                                done_ok.append(arr[0])
                        elif arr[1] == 'failed':
                                failed.append(arr[0])
                        elif arr[1] == 'aborted':
                                aborted.append(arr[0])
                        elif arr[1] == 'stat_failed':
                                stat_failed.append(arr[0])
                        elif arr[1] == 'submit_failed':
                                submit_failed.append(arr[0])
                        elif arr[1] == 'cancel_failed':
                                cancel_failed.append(arr[0])
                        elif 'illegal' in arr[1]:
                                illegal.append(arr[0])
                        else:
                                print "###INVALID OLD STATE RECIEVED (UNKNOWN STATE):" + arr[1] + " FOR JID: " + arr[0]
                                print "(this is possibly due to a bug)"

                        #remove any jobs lost in transit from the running queue
                        if arr[1] in ['cancelled', 'done_ok', 'failed', 'aborted']:
                                if arr[0] in running:
                                        running.remove(arr[0])

                timestamp = datetime.datetime.now()
                elapsed = timestamp - start
                remaining = duration*60 - elapsed.seconds

                finished = len(cancelled)+len(done_ok)+len(failed)+len(aborted)+len(stat_failed)+len(submit_failed)

                message = "Submitted: " + str(len(submitted)) + " Running: " + str(len(running)) + " Cancelled: " + str(len(cancelled)) +\
                          " Done-Ok: " + str(len(done_ok)) + " Failed: " + str(len(failed)) + " Aborted: " + str(len(aborted)) +\
                          " \n\t\t\t\tStatus Failed: " + str(len(stat_failed)) + " Submit Failed: " + str(len(submit_failed)) +\
                          " Cancel Failed: " + str(len(cancel_failed)) + " Illegal Final Status: " + str(len(illegal))
                if duration > 0 :
                        message += " \n\t\t\t\tTime left: " + str(remaining/60) + " minutes"
                else:
                        message += " \n\t\t\t\tCollected " + str(finished) + "/" + str(numJobs)

                sp.safe_print(message, None)
                #print str(timestamp) + '\t! ' + message

                if (finished == numJobs and duration == 0) or (duration > 0 and elapsed.seconds >= duration*60 and len(running) == 0) :

                        for pid in pids: #wait for submitter procs to end
                                while os.path.exists('/proc/'+str(pid)):
                                        pass #busy?

                        message = '\nAll jobs have been collected!' + '\n' +\
                                  '\nTotal Jobs: ' + str(numJobs) + '\n' +\
                                  'DONE-OK: ' + str(len(done_ok)) + '\n' +\
                                  'DONE-FAILED: ' + str(len(failed)) + '\n' +\
                                  'ABORTED: ' + str(len(aborted)) + '\n' +\
                                  'CANCELLED: ' + str(len(cancelled)) + '\n' +\
                                  'Failed Submit operations: ' + str(len(submit_failed)) + '\n' +\
                                  'Failed Cancel operations: ' + str(len(cancel_failed)) + '\n' +\
                                  'Failed Status operations: ' + str(len(stat_failed)) + '\n' +\
                                  'Illegal Final Status: ' + str(len(illegal))

                        #sp.safe_print(message,None)
                        print message

                        sys.exit(0)
###############################################################################################################
###############################################################################################################
def submit_wrapper(jdl_path, endpoint, deleg_id, sp, q, jobs_left, my_jobs_left, start, duration, now):
        '''
                submit job wrapper function, mainly to implement fault tolerance and reporting
        '''

        failed_submit_ops=0
        ex=False
        while True:
                try:
                        jid=submit_job(jdl_path, endpoint, deleg_id)
                        break
                except Exception as e:
                        failed_submit_ops +=1
                        if failed_submit_ops < 5: #accept some failures due to high traffic (con timed out etc)
                                time.sleep(random.randrange(1,4))
                        else: #if too many failures, report and log it
                                ex=True
                                sp.safe_print('Submit operation failed for reason:\n' + str(e) , 'red')
                                q.add(['not-available','submit_failed'])
                                break

        if ex == True:
                return (None, 'fail')
        if ex == False:
                #submission successful, log it and store the jid as submitted, running
                if duration > 0:
                        elapsed = now - start
                        remaining = duration*60 - elapsed.seconds

                        sp.safe_print('Submit\t' + jid + ' Time Left at submission time: ' +
                                      str(remaining/60) + ' minutes', 'blue')
                else:
                        sp.safe_print('Submit\t' + jid + ' Left: ' + str(jobs_left) +
                                      ' (at submission time:' + str(my_jobs_left) + ')', 'blue')

                q.add([jid,'submitted'])

                return (jid, 'success')

###############################################################################################################
###############################################################################################################
def cancel_wrapper(jid, sp, q):
        '''
                cancel job wrapper function, mainly to implement fault tolerance and reporting
        '''

        failed_cancel_ops=0
        ex=False
        while True:
                try:
                        cancel_job(jid)
                        break
                except Exception as e:
                        failed_cancel_ops +=1
                        if failed_cancel_ops < 5: #accept some failures due to high traffic (con timed out etc)
                                time.sleep(random.randrange(1,4))
                        else: #if too many failures, report and log it
                                ex=True
                                sp.safe_print('Cancel\t' + jid + ' Error Message: ' + str(e), 'red')
                                q.add([jid,'cancel_failed']) ;
                                break

        if ex == True: #cancel ultimately failed, already reported it, but WAIT to collect job status
                return 'fail'
        if ex == False: #cancel successful, log it and store the jid at the cancelled
                sp.safe_print('Cancel\t' + jid, 'blue')
                return 'ok'
###############################################################################################################
###############################################################################################################
def final_status_wrapper(jid, sp, q, output_dir):
        '''
                job final status wrapper function, mainly to implement fault tolerance and reporting
        '''

        failed_status_ops=0
        ex=False
        while True:
                try:
                        (status, log) = get_final_status(jid)
                        break
                except Exception as e:
                        failed_status_ops += 1
                        if failed_status_ops < 5: #accept some failures due to high traffic (con timed out etc)
                                time.sleep(10)
                        else: # if too many failures, report and log it
                                ex=True
                                sp.safe_print('Status operation error on: ' + jid, 'red')
                                q.add([jid,'stat_failed'])

                                fName = output_dir + "CREAM" + jid.split('CREAM')[1]  # log the failure instead of the job status
                                fp = open(fName,'w')
                                fp.write(str(e).strip() + '\n')
                                fp.close()

                                break

        if ex == True:          #couldn't determin job status, already reported it, so move to next iteration
                status = 'failed_to_determine'
                return (status, 'fail')
        if ex == False: #job finished (regardless of state), log it and store the jid at the finished, remove from running
                sp.safe_print('Exit\t' + jid + ' Status: "' + status + '"', "magenta")

                fName = output_dir + "CREAM" + jid.split('CREAM')[1]
                fp = open(fName,'w')
                for item in log:
                        fp.write(item)
                fp.close()

                return (status, 'ok')
###############################################################################################################
###############################################################################################################
def job_status_wrapper(jid, sp, q, output_dir):
        '''
                Gather the status of a job and return it.
                If the status fails to be determined and/or an error happens, log it.
                If the status is final, log it.
        '''

        final_states = ['DONE-OK', 'DONE-FAILED', 'ABORTED', 'CANCELLED']

        failed_status_ops=0
        ex=False
        while True:
                try:
                        #(status, log) = get_final_status(jid)
                        (status, log) = get_current_status(jid,2)
                        break
                except Exception as e:
                        failed_status_ops += 1
                        if failed_status_ops < 5: #accept some failures due to high traffic (con timed out etc)
                                time.sleep(10)
                        else: # if too many failures, report and log it
                                ex=True
                                sp.safe_print('Status operation error on: ' + jid, 'red')
                                q.add([jid,'stat_failed'])

                                fName = output_dir + "CREAM" + jid.split('CREAM')[1]  # log the failure instead of the job status
                                fp = open(fName,'w')
                                fp.write(str(e).strip() + '\n')
                                fp.close()

                                break

        if ex == True:          #couldn't determin job status, already reported it, so move to next iteration
                status = 'failed_to_determine'
                return (status, 'fail')
        if ex == False: #log or simply return info, depending on job's state
                if status in final_states:
                        sp.safe_print('Exit\t' + jid + ' Status: "' + status + '"', "magenta")

                        fName = output_dir + "CREAM" + jid.split('CREAM')[1]
                        fp = open(fName,'w')
                        for item in log:
                                fp.write(item)
                        fp.close()

                return (status, 'ok')
###############################################################################################################
###############################################################################################################
def create_delegation(cream_endpoint,delegId):
        '''
                |  Description: |   Delegate user's proxy credentials,to be used later for job submissions. | \n
                |  Arguments:   |   cream_endpoint     |     the cream endpoint                             |
                |               |   delegId            |     the delegation id string                       | \n
                |  Returns:     |   nothing                                                                 |
                NOTE: taken from cream functional testing suite
        '''

        com="/usr/bin/glite-ce-delegate-proxy -e %s %s" % (cream_endpoint,delegId)
        args = shlex.split(com.encode('ascii'))
        p = subprocess.Popen( args , shell=False , stderr=subprocess.STDOUT , stdout=subprocess.PIPE )
        fPtr=p.stdout

        retVal=p.wait()

        output=fPtr.readlines()

        if retVal != 0 :
                raise _error("Delegation failed.Command output:\n %s" % output)
###############################################################################################################
###############################################################################################################
def get_final_status(job_id):
        '''
                |  Description: |   Return the final status of a job,with the use of the glite-ce-job-status command.   |
                |               |   This command will wait until the job is in a final state.                           | \n
                |  Arguments:   |   job_id     |     the job id,as returned by the submit operation.                    | \n
                |  Returns:     |   job's final status as a string.                                                     |
                NOTE: taken from cream functional testing suite
        '''

        running_states = ['IDLE','REGISTERED', 'PENDING', 'RUNNING', 'REALLY-RUNNING', 'HELD']
        final_states = ['DONE-OK', 'DONE-FAILED', 'ABORTED', 'CANCELLED']

        com="glite-ce-job-status -d -L 2 " + job_id
        args = shlex.split(com.encode('ascii'))

        while 1 :
                p = subprocess.Popen( args , stderr=subprocess.STDOUT , stdout=subprocess.PIPE )
                fPtr=p.stdout

                retVal=p.wait()

                output=fPtr.readlines()

                if retVal != 0 or ("FaultCause" in output and "ErrorCode" in output):
                        raise _error("Job status polling failed with return value: " + str(p.returncode) + "\nCommand reported: " + repr(output))


                found=False
                for i in output:
                        if "Status" in i:
                                status_line=i
                                found=True
                                break

                if found == False:
                    raise _error("Status couldn't be determined for jid " + job_id + ". Command reported: " + ','.join(output))

                cur_status = status_line.split('[')[1].split(']')[0]

                #print "Found state: " + cur_status + " for jid: " + job_id
                if cur_status in final_states :
                        return (cur_status, output)
                else:
                        time.sleep(random.randrange(5,15)) #sleep a semi-random amount of time in order to not overflow the server
###############################################################################################################
###############################################################################################################
def get_current_status(job_id, verbosity='2'):
        '''
                |  Description:  |  Return the current status of a job,with the use of the glite-ce-job-status command,with the given verbosity  |
                |                |  This function will NOT wait until the job is in a final state.                                               | \n
                |  Arguments:    |  job_id      |    as returned by the submit operation.                                                        |
                |                |  verbosity   |    0,1 or 2                                                                                    |
                |  Returns:      |  2 strings, the current state and the full output.                                                            |
                NOTE: taken from cream functional testing suite
        '''

        running_states = ['IDLE','REGISTERED', 'PENDING', 'RUNNING', 'REALLY-RUNNING', 'HELD']
        final_states = ['DONE-OK', 'DONE-FAILED', 'ABORTED', 'CANCELLED']

        com="glite-ce-job-status -L " + str(verbosity) + " " + job_id
        args = shlex.split(com.encode('ascii'))

        p = subprocess.Popen( args , stderr=subprocess.STDOUT , stdout=subprocess.PIPE )
        fPtr=p.stdout

        output=fPtr.readlines()

        p.wait()

        if p.returncode != 0 or ("FaultCause" in output and "ErrorCode" in output):
                raise _error("Job status polling failed with return value: " + str(p.returncode) + "\nCommand reported: " + output)

        found=False
        for i in output:
                if "Status" in i:
                        status_line=i
                        found=True
                        break

        if found == False:
            raise _error("Status couldn't be determined for jid " + job_id + ". Command reported: " + ','.join(output))

        cur_status = status_line.split('[')[1].split(']')[0]

        return (cur_status, output)
###############################################################################################################
###############################################################################################################
def submit_job(jdl_path,ce_endpoint,delegId=None):
        '''
                |  Description: |   Submit a job with automatic or explicit delegation and return it's job id.                      | \n
                |  Arguments:   |   jdl_path      |  path to the jdl file                                                           |
                |               |   ce_endpoint   |  the cream endpoint,containing the queue                                        |
                |               |   delegId       |  if specified,uses the given delegation id, else it uses automatic delegation   | \n
                |  Returns:     |   the resulting cream job id as a string                                                          |
                NOTE: taken from cream functional testing suite
        '''

        if delegId is None:
                com="/usr/bin/glite-ce-job-submit -d -a -r " + ce_endpoint + " " + jdl_path
        else:
                com="/usr/bin/glite-ce-job-submit -d -r " + ce_endpoint + " -D " + delegId + " " + jdl_path

        args = shlex.split(com.encode('ascii'))

        p = subprocess.Popen( args , shell=False , stderr=subprocess.STDOUT , stdout=subprocess.PIPE )
        fPtr=p.stdout

        retVal=p.wait()

        output=fPtr.readlines()

        #if retVal != 0:
        if "error" in ','.join(output) or "fault" in ','.join(output) or retVal != 0 :
            raise _error("Job submission failed with return value: " + str(p.returncode) + " \nCommand reported: " +  ','.join(output) )

        jid=output[-1] #if job submission was succesfull (at this point of code,it is),then the last line of output holds the job id
        jid=jid[:-1]   #to remove the trailing '\n'

        return jid
###############################################################################################################
###############################################################################################################
def simple_jdl(output_dir):
        '''
                |  Description:  |  Simple jdl file.Executes /bin/uname -a.             | \n
                |  Arguments:    |  output_dir   |   the directory to put the file in   | \n
                |  Returns:      |  Temporary file name.                                |
                NOTE: taken from cream functional testing suite
        '''

        folder = output_dir
        identifier = 'simple'
        name = 'cream_stress_testing-' + str(time.time()) + '-' + identifier + '.jdl'
        path = folder + '/' + name

        jdl_file = open(path,'w')

        jdl_contents =  '[\n'\
                        'Type="job";\n'\
                        'JobType="normal";\n'\
                        'Executable="/bin/uname";\n'\
                        'Arguments="-a";\n'\
                        'StdOutput="job.out";\n'\
                        'StdError="job.err";\n'\
                        ']\n'


        jdl_file.write(jdl_contents)
        jdl_file.close()

        return path
###############################################################################################################
###############################################################################################################
def sleep_jdl(output_dir, sleep_time):
        '''
                |  Description:  |  Sleep jdl file.Executes /bin/sleep Xs               | \n
                |  Arguments:    |  output_dir   |   the directory to put the file in   |
                |                |  sleep_time   |   the time to sleep in seconds       |
                |  Returns:      |  Temporary file name.                                |
                NOTE: taken from cream functional testing suite
        '''

        folder = output_dir
        identifier = 'sleep'
        name = 'cream_stress_testing-' + str(time.time()) + '-' + identifier + '.jdl'
        path = folder + '/' + name

        jdl_file = open(path,'w')

        jdl_contents =  '[\n'\
                        'Type="job";\n'\
                        'JobType="normal";\n'\
                        'Executable="/bin/sleep";\n'\
                        'Arguments="'+str(sleep_time)+'";\n'\
                        'StdOutput="job.out";\n'\
                        'StdError="job.err";\n'\
                        ']\n'


        jdl_file.write(jdl_contents)
        jdl_file.close()

        return path
###############################################################################################################
###############################################################################################################
def cancel_job(job_id):
        '''
                |  Description:  |  Cancel the given job.                                       | \n
                |  Arguments:    |  job_id     |    as returned by the submit operation.        | \n
                |  Returns:      |  nothing.                                                    |
                NOTE: taken from cream functional testing suite
        '''

        failed_stats=0
        status=''
        #while status not in ['IDLE', 'RUNNING', 'REALLY-RUNNING', 'HELD', 'PENDING']:
        while status not in ['IDLE', 'RUNNING', 'REALLY-RUNNING', 'HELD']:
        #while status not in ['REALLY-RUNNING', 'HELD']:
                try:
                        (status, log) = get_current_status(job_id,0)

                        #if a job somehow ended up in a final state, continue to the cancel operation (cancel it) to force the error to appear
                        if status in ['DONE-OK', 'DONE-FAILED', 'ABORTED', 'CANCELLED']:  
                                break
                except Exception as e:
                        #print "ERROR ON JOB CANCEL STATUS OP " + str(e)
                        status=''
                        failed_stats+=1
                        if failed_stats > 5:
                                message = "Job cancel operation failed because it couldn't determine the job's status" +\
                                          "Job cancel operation return value: " +\
                                          str(p.returncode) + " \nCommand reported: " +  output 
                                raise _error(message)

        com="/usr/bin/glite-ce-job-cancel -d -N " + job_id
        args = shlex.split(com.encode('ascii'))
        p = subprocess.Popen( args , shell=False , stderr=subprocess.STDOUT , stdout=subprocess.PIPE )
        fPtr=p.stdout
        retVal=p.wait()
        output=fPtr.read()

        if "error" in output.lower() or "fatal" in output.lower() or "fault" in output.lower() or retVal != 0 :
                raise _error("Job cancel operation failed with return value: " + str(p.returncode) + " \nCommand reported: " +  output )

        #print "Job had status: " + status + " when cancelled"
        #print "Cancel output: " + output
##############################################################################################################################
##############################################################################################################################
def check_proxy(time_left=None):
        '''
                |  Description:  |  Check whether the proxy exists and if it has any time left.                                                  |\n
                |  Arguments:    |  Without any arguments,it checks if the proxy exists and has any time left                                    |
                |                |  With one argument,it checks if the proxy exists and has greater than or equal to the requested time left.    |\n
                |  Returns:      |  nothing                                                                                                      |
                NOTE: taken from cream functional testing suite
        '''


        if os.environ.has_key("X509_USER_PROXY") == False :
                raise _error("Proxy path env. var not set")

        if os.path.exists(os.environ["X509_USER_PROXY"]) == False :
                raise _error('Proxy file "'+ os.environ["X509_USER_PROXY"] +'" not present or inaccessible')

        com="/usr/bin/voms-proxy-info -timeleft"
        args = shlex.split(com.encode('ascii'))
        p = subprocess.Popen( args , shell=False , stderr=subprocess.STDOUT , stdout=subprocess.PIPE )
        fPtr=p.stdout
        proxy_timeleft=int(fPtr.readline())

        if time_left == None:
                if proxy_timeleft <= 0 :
                        raise _error("No proxy time left")
        else:
                if proxy_timeleft <= int(time_left) :
                        raise _error("Proxy has less time than requested (%s seconds) left" % time_left)
##############################################################################################################################
##############################################################################################################################
def parse_arguments():
        '''
                Command line argument parsing routine
        '''

        parser = argparse.ArgumentParser(description = 'Script performing submit, cancel and lease operations to a CREAM endpoint.',
                                         epilog      = 'For help, bugs, etc contact the author at dfiore -at- noc -dot- edunet -dot- gr')
        parser.add_argument('-n','--numjobs',
               required=True,
               help='The total number of jobs to run',
               type=int,
               dest='numJobs')
        parser.add_argument('-c','--concurrentSubmitters',
               default=1,
               help='The number of total submitter processes to create. Defaults to 1.',
               type=int,
               dest='concSubm')
        parser.add_argument('-s','--slots',
               default=1,
               help='The number of jobs each submitter processes will submit/manage. Defaults to 1.',
               type=int,
               dest='slots')
        parser.add_argument('--nocolor',
               help='If you do not wish to use colored output, specify this flag.',
               action='store_true',
               dest='no_color')
        parser.add_argument('--noprint',
               help='If you do not wish to print output, specify this flag.',
               action='store_true',
               dest='no_print')
        parser.add_argument('--constant',
               default=0,
               help='Specify this option, in order to keep a constant queue of the number of jobs provided to the -n (--numjobs) option.'+\
                    'Additional time argument required, an integer in minutes, which represents the time the job queue will be sustained.'+\
                    'Note that if the number of concurrent submitters isn\'t a divisor of the number of jobs, then '+\
                    'the number of slots (-s) will be forcefully set to numJobs/concSubmitters and then numJobs will be forcefully '+\
                    'set to slots*concSubmitters',
               type=int,
               dest='constant')
        parser.add_argument('--sleep',
               default=0,
               help='The time to sleep, in case of a sleep jdl being used. Defaults to 0 sec.',
               type=int,
               dest='sleepTime')
        parser.add_argument('-e','--endpoint',
               required=True,
               help='The endpoint on which to run. Example: ctb04.gridctb.uoa.gr:8443/cream-pbs-see',
               type=str,
               dest='endpoint')
        parser.add_argument('-j','--jdl',
               default='notset',
               help='Path to the jdl file to use. If none is given, a sample builtin jdl running uname will be used.'+\
                    'If "sleep" is given, then a jdl which sleeps for (--sleep) seconds will be created.'+\
                    'Note: the --sleep argument defaults to 0, so it should essentially be given if the jdl is supposed to actually sleep',
               type=str,
               dest='jdl_path')
        deleg_valid_values=['multiple','single']
        parser.add_argument('-d','--delegation',
               default='multiple',
               help='The type of delegation to use for the jobs. Either multiple or single. Defaults to multiple.',
               type=str,
               dest='delegType',
               choices=deleg_valid_values)
        test_type_valid_values=['submit','cancel','lease']
        parser.add_argument('-t','--testtype',
               required=True, help='The type of test to run. Either "submit", "cancel" or "lease".',
               type=str,
               dest='testType',
               choices=test_type_valid_values)

        args = parser.parse_args()

        if args.concSubm < 1 or args.slots < 1 or args.numJobs < 1:
                raise _error('Integer arguments aren\'t allowed a value less than 1. Please review the arguments passed.')

#        if args.no_color is True:
#                YELLOW='' ; RED='' ; MAGENTA='' ; BLUE=''
#                BLACK='' ; GREY='' ; END=''

        if args.concSubm*args.slots > args.numJobs:
                args.numJobs = args.concSubm*args.slots
                print "Total number of jobs increased to: " + str(args.concSubm*args.slots)
#        if args.slots > 1:
#                if args.concSubm*args.slots < args.numJobs:
#                        args.numJobs = args.concSubm*args.slots
#                        print "Total number of jobs decreased to: " + str(args.concSubm*args.slots)


        pattern = '.+\:\d\d\d\d\/.*'
        match = re.search(pattern,args.endpoint)
        if not match:
                raise _error('Endpoint given is not in the right format. Example: ' +\
                             'ctb04.gridctb.uoa.gr:8443/cream-pbs-see. You entered:"' + args.endpoint + '"')

        if args.testType != 'submit' and args.testType != 'cancel':
                raise _error("Only submit and cancel test is supported for the time being")

        if args.constant < 0:
                raise _error("Invalid (negative) value for argument \"--constant\"")
        elif args.constant > 0:
                args.slots = args.numJobs / args.concSubm
                args.numJobs = args.concSubm * args.slots
                print "Will run in constant mode, for " + str(args.constant) + " minutes."
                print "Number of jobs: " + str(args.numJobs) + " Submitter processes: " +\
                      str(args.concSubm) + " Jobs per subimtter: " + str(args.slots)

        return args
###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################
###############################################################################################################


if __name__ == '__main__':
        '''
                the main function
        '''

        check_proxy()

        # setup stuff start here

        args = parse_arguments()

        if args.jdl_path == 'notset':
                print 'Will create simple jdl'
                jdl_path = simple_jdl('/tmp/')
        elif args.jdl_path == 'sleep':
                print 'Will create sleep jdl (' + str(args.sleepTime) + ' seconds)'
                jdl_path = sleep_jdl('/tmp',args.sleepTime)
        else:
                jdl_path = args.jdl_path

        if args.delegType == 'multiple':
                deleg_id = None
        else:
                print "Will create single delegation"
                deleg_id = ''.join(random.choice(string.letters) for i in xrange(15))
                create_delegation(args.endpoint.split('/')[0],deleg_id)

        sp = sync_print()
        if args.no_color is True:
                sp.disable_color()
        if args.no_print is True:
                sp.disable_print()

        # create temp rand dir inside /tmp to hold status logs
        tmp_dir = tempfile.mkdtemp(suffix=".cream_stress_testing", dir="/tmp/") + '/'
        print "Temporary directory is: " + tmp_dir

        # setup stuff ends here

        # multiprocess stuff start here

        # create my locks
        var_lock = Lock()

        # shared vars
        jobs_left = Value('i', args.numJobs)
        q = sync_queue()

        start = datetime.datetime.now()
        print 'Starting at: ' + start.strftime('%c') + '\n\n\n\n'

        # Submitting jobs part
        submitters=[]

        p=Process(target=g, args=(q, sp, args.numJobs, start, args.constant))        # the supervisor proc
        submitters.append(p)
        p.start()

        for num in range(args.concSubm):                # the submitter procs
                p=Process(target=f, args=(var_lock, sp, num, jobs_left,
                          jdl_path, args.endpoint, deleg_id, tmp_dir, args.testType,
                          q, args.slots, start, args.constant))
                submitters.append(p)
                p.start()

        proc_list = multiprocessing.active_children()
        while ( len(proc_list) > 0 ):
            proc_list = multiprocessing.active_children()
            time.sleep(5) #non-busy wait

        end = datetime.datetime.now()
        print '\nEnded at: ' + end.strftime('%c')
        elapsed = end - start
        print 'Time elapsed: ' + str(elapsed)
        print '\nOutput stored at: ' + tmp_dir
